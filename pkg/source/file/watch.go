/*
Copyright 2021 Loggie Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package file

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/util/persistence/reg"

	"github.com/fsnotify/fsnotify"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/external"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/loggie-io/loggie/pkg/util/persistence"
)

const (
	CREATE = Operation(0)
	WRITE  = Operation(1)
	REMOVE = Operation(2)
	RENAME = Operation(3)
)

type Operation int

type jobEvent struct {
	opt         Operation
	job         *Job
	newFilename string
	newFileSize int64
}

type Watcher struct {
	done                   chan struct{}
	config                 WatchConfig
	sourceWatchTasks       map[string]*WatchTask // key:pipelineName:sourceName
	waiteForStopWatchTasks map[string]*WatchTask
	watchTaskEventChan     chan WatchTaskEvent
	osWatcher              *fsnotify.Watcher
	osWatchFiles           map[string]bool // key:file|value:1;only zombie job file need os notify
	allJobs                map[string]*Job // key:`pipelineName:sourceName:job.Uid`|value:*job
	zombieJobs             map[string]*Job // key:`pipelineName:sourceName:job.Uid`|value:*job
	currentOpenFds         int
	zombieJobChan          chan *Job
	dbHandler              *persistence.DbHandler
	countDown              *sync.WaitGroup
	stopOnce               *sync.Once
}

func newWatcher(config WatchConfig, dbHandler *persistence.DbHandler) *Watcher {
	w := &Watcher{
		done:                   make(chan struct{}),
		config:                 config,
		sourceWatchTasks:       make(map[string]*WatchTask),
		waiteForStopWatchTasks: make(map[string]*WatchTask),
		watchTaskEventChan:     make(chan WatchTaskEvent),
		dbHandler:              dbHandler,
		zombieJobChan:          make(chan *Job, config.MaxOpenFds+1),
		allJobs:                make(map[string]*Job),
		osWatchFiles:           make(map[string]bool),
		zombieJobs:             make(map[string]*Job),
		countDown:              &sync.WaitGroup{},
		stopOnce:               &sync.Once{},
	}
	w.initOsWatcher()
	go w.run()
	return w
}

func (w *Watcher) Stop() {
	w.stopOnce.Do(func() {
		log.Info("start stop watcher")
		close(w.done)
		w.countDown.Wait()
		if w.osWatcher != nil {
			err := w.osWatcher.Close()
			if err != nil {
				log.Error("stop watcher fail: %v", err)
			}
		}
		// clean data
		for _, job := range w.allJobs {
			job.Stop()
			w.finalizeJob(job)
		}
		log.Info("watcher stop")
	})
}

func (w *Watcher) StopWatchTask(watchTask *WatchTask) {
	watchTask.stopTime = time.Now()
	watchTask.countDown.Add(1)
	w.watchTaskEventChan <- WatchTaskEvent{
		watchTaskType: STOP,
		watchTask:     watchTask,
	}
	watchTask.countDown.Wait()
	stopCost := time.Since(watchTask.stopTime)
	if stopCost > 10*time.Second {
		log.Warn("watchTask(%s) stop cost: %ds", watchTask.String(), stopCost/time.Second)
	}
}

func (w *Watcher) StartWatchTask(watchTask *WatchTask) {
	w.watchTaskEventChan <- WatchTaskEvent{
		watchTaskType: START,
		watchTask:     watchTask,
	}
}

func (w *Watcher) preAllocationOffset(size int64, job *Job) {
	w.dbHandler.HandleOpt(persistence.DbOpt{
		R: reg.Registry{
			PipelineName: job.task.pipelineName,
			SourceName:   job.task.sourceName,
			Filename:     job.filename,
			JobUid:       job.Uid(),
			Offset:       size,
		},
		OptType:     persistence.UpsertOffsetByJobWatchIdOpt,
		Immediately: true,
	})
}

func (w *Watcher) findExistJobRegistry(job *Job) reg.Registry {
	return w.dbHandler.FindBy(job.Uid(), job.task.sourceName, job.task.pipelineName)
}

func (w *Watcher) initOsWatcher() {
	if !w.config.EnableOsWatch {
		return
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Error("registry os notify fail: %v", err)
	} else {
		w.osWatcher = watcher
	}
}

func (w *Watcher) addOsNotify(file string) {
	if _, ok := w.osWatchFiles[file]; ok {
		return
	}
	w.osWatchFiles[file] = true
	if w.osWatcher != nil {
		err := w.osWatcher.Add(file)
		if err != nil {
			log.Warn("add file(%s) os notify fail: %v", file, err)
		}
	}
}

func (w *Watcher) removeOsNotify(file string) {
	if _, ok := w.osWatchFiles[file]; !ok {
		return
	}
	delete(w.osWatchFiles, file)
	if w.osWatcher != nil {
		err := w.osWatcher.Remove(file)
		if err != nil {
			log.Debug("remove file(%s) os notify fail: %v", file, err)
		}
	}
}

// DecideJob should be as lightweight as possible.
// Operations such as releasing file handles should be placed in a separate goroutine of watch
func (w *Watcher) DecideJob(job *Job) {
	job.Sync()

	w.reportMetric(job)

	// Stopped jobs are directly put into the zombie queue for release
	if job.IsStop() {
		w.zombieJobChan <- job
		return
	}
	// inactive
	if job.EofCount > w.config.MaxEofCount {
		w.zombieJobChan <- job
		return
	}
	// w.activeChan <- job
	job.Read()
}

func (w *Watcher) reportMetric(job *Job) {
	if job.endOffset == 0 {
		// file is not really being collected
		return
	}
	collectMetricData := eventbus.CollectMetricData{
		BaseMetric: eventbus.BaseMetric{
			PipelineName: job.task.pipelineName,
			SourceName:   job.task.sourceName,
		},
		FileName:     job.filename,
		Offset:       job.endOffset,
		LineNumber:   job.currentLineNumber,
		Lines:        job.currentLines,
		SourceFields: job.task.sourceFields,
	}
	job.currentLines = 0
	eventbus.PublishOrDrop(eventbus.FileSourceMetricTopic, collectMetricData)
}

func (w *Watcher) eventBus(e jobEvent) {
	job := e.job
	filename := job.filename

	switch e.opt {
	case REMOVE:
		log.Info("fileName(%s) with uid(%s) was removed", filename, job.Uid())
		// cannot ignore call job.Delete()
		job.Delete()
		// More aggressive handling of deleted files
		if w.isZombieJob(job) {
			w.finalizeJob(job)
		}
	case RENAME:
		job.RenameTo(e.newFilename)
		log.Info("job fileName(%s) rename to %s", filename, e.newFilename)
		if w.isZombieJob(job) && job.file == nil {
			w.handleRenameJobs(job)
		}
		// waiting to write to registry
	case WRITE:
		if job.IsStop() {
			return
		}
		// only care about zombie job write event
		watchJobId := job.WatchUid()
		existJob, ok := w.zombieJobs[watchJobId]
		if !ok {
			return
		}

		// check whether the file size is less than the offset in the job
		filesize := e.newFileSize
		currentOffset := job.endOffset
		if filesize < currentOffset {
			// maybe the file is truncated
			log.Info("filesize: %d, currentOffset: %d", filesize, currentOffset)
			existRegistry := w.findExistJobRegistry(job)
			existAckOffset := existRegistry.Offset
			if existAckOffset > filesize+int64(len(job.GetEncodeLineEnd())) {
				log.Warn("the job(jobUid:%s) fileName(%s) existRegistry(%+v) ackOffset is larger than file size(%d), the file was truncate", job.Uid(), filename, existRegistry, filesize)
				// file was truncated, need to reinitialize the job
				job.Delete()
				if w.isZombieJob(job) {
					w.finalizeJob(job)
				}
				return
			}
		}

		err, fdOpen := existJob.Active()
		if fdOpen {
			w.currentOpenFds++
		}
		if err != nil {
			log.Error("active job fileName(%s) fail: %s", filename, err)
			if existJob.Release() {
				w.currentOpenFds--
			}
			return
		}
		existJob.Read()
		// zombie job change to active, so without os notify
		w.removeOsNotify(existJob.filename)
		log.Debug("job fileName(%s) change to active", filename)
		delete(w.zombieJobs, watchJobId)

	case CREATE:
		if w.currentOpenFds >= w.config.MaxOpenFds {
			log.Error("maxCollectFiles reached. fileName(%s) will be ignore", filename)
			return
		}
		watchJobId := job.WatchUid()
		if _, ok := w.allJobs[watchJobId]; ok {
			return
		}
		stat, err := os.Stat(filename)
		if err != nil {
			log.Error("create job fileName(%s) fail: %s", filename, err)
			return
		}
		existRegistry := w.findExistJobRegistry(job)
		existAckOffset := existRegistry.Offset
		existLineNumber := existRegistry.LineNumber
		fileSize := stat.Size()
		// check whether the existAckOffset is larger than the file size
		if existAckOffset > fileSize+int64(len(job.GetEncodeLineEnd())) {
			log.Warn("new job(jobUid:%s) fileName(%s) existRegistry(%+v) ackOffset is larger than file size(%d).is inode repeat?", job.Uid(), filename, existRegistry, fileSize)
			// file was truncated，start from the beginning
			if job.task.config.RereadTruncated {
				existAckOffset = 0
				existLineNumber = 0
			}
		}
		// Pre-allocation offset
		if existAckOffset == 0 || e.job.task.config.ReadFromTail {
			if e.job.task.config.ReadFromTail {
				existAckOffset = fileSize
			}
			w.preAllocationOffset(existAckOffset, job)
		}
		// set ack offset
		job.NextOffset(existAckOffset)
		// set line number
		job.currentLineNumber = existLineNumber
		// active job
		err, fdOpen := job.Active()
		if fdOpen {
			w.currentOpenFds++
		}
		if err != nil {
			log.Error("active job fileName(%s) fail: %s", filename, err)
			if job.Release() {
				w.currentOpenFds--
			}
			return
		}
		w.allJobs[watchJobId] = job
		job.Read()
		if existAckOffset > 0 {
			log.Info("[%s-%s] start collect file from existFileName(%s) with existOffset(%d): %s", job.task.pipelineName, job.task.sourceName, existRegistry.Filename, existAckOffset, job.filename)
		} else {
			log.Info("[%s-%s] start collect file: %s", job.task.pipelineName, job.task.sourceName, job.filename)
		}
		// ignore OS notify of path because it will cause too many system notifications
	}
}

func ignoreSystemFile(fileName string) bool {
	return fileName == "" || fileName == "." || fileName == ".." || fileName == "/"
}

func (w *Watcher) cleanWatchTaskRegistry(watchTask *WatchTask) {
	if !w.config.CleanWhenRemoved {
		return
	}
	registries := w.dbHandler.FindAll()
	for _, r := range registries {
		if r.PipelineName == watchTask.pipelineName && r.SourceName == watchTask.sourceName {
			// file remove?
			_, err := os.Stat(r.Filename)
			if err != nil && os.IsNotExist(err) {
				// delete registry
				w.dbHandler.HandleOpt(persistence.DbOpt{
					R:           r,
					OptType:     persistence.DeleteByIdOpt,
					Immediately: true,
				})
			}
		}
	}
}

func (w *Watcher) scanNewFiles() {
	for _, watchTask := range w.sourceWatchTasks {
		w.scanTaskNewFiles(watchTask)
	}
}

func (w *Watcher) scanTaskNewFiles(watchTask *WatchTask) {
	pipelineName := watchTask.pipelineName
	sourceName := watchTask.sourceName
	paths := watchTask.config.Paths
	if isDynamicPath(paths) {
		w.scanDynamicContainerLogs(pipelineName, sourceName, watchTask)
		return
	}

	w.scanPaths(pipelineName, sourceName, paths, watchTask, nil)
}

func isDynamicPath(paths []string) bool {
	if len(paths) == 1 && paths[0] == external.SystemContainerLogsPath {
		return true
	}
	return false
}

func getPathsIfDynamicContainerLogs(paths []string, pipelineName string, sourceName string) []string {
	if !isDynamicPath(paths) {
		return paths
	}

	pairs, ok := external.GetDynamicPaths(pipelineName, sourceName)
	if !ok {
		log.Debug("cannot get dynamic paths by %s/%s", pipelineName, sourceName)
		return paths
	}

	var dynamicPaths []string
	for _, p := range pairs {
		dynamicPaths = append(dynamicPaths, p.Paths...)
	}

	return dynamicPaths
}

func (w *Watcher) scanDynamicContainerLogs(pipelineName string, sourceName string, watchTask *WatchTask) {
	pairs, ok := external.GetDynamicPaths(pipelineName, sourceName)
	if !ok {
		log.Info("cannot get dynamic paths by %s/%s", pipelineName, sourceName)
		return
	}

	for _, pair := range pairs {
		path := getRecursivePath(pair.Paths)
		w.scanPaths(pipelineName, sourceName, path, watchTask, pair.Fields)
	}

	return
}

func (w *Watcher) scanPaths(pipelineName string, sourceName string, paths []string, watchTask *WatchTask, jobFields map[string]interface{}) {
	for _, path := range paths {
		matches, err := util.GlobWithRecursive(path)
		log.Debug("scan paths %+v , matches: %+v", path, matches)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			log.Info("[pipeline(%s)-source(%s)]: glob path(%s) fail: %v", pipelineName, sourceName, path, err)
			continue
		}
		for _, fileName := range matches {
			w.createOrRename(fileName, watchTask, jobFields)
		}
	}
}

func (w *Watcher) createOrRename(filename string, watchTask *WatchTask, jobFields map[string]interface{}) {
	if legal, name, fileInfo := w.legalFile(filename, watchTask, true); legal {
		job := watchTask.newJob(name, fileInfo)
		job.jobFields = jobFields

		err := job.GenerateIdentifier()
		if err != nil {
			log.Info("file(%s) ignored: %s", name, err)
			return
		}
		existJob, ok := w.allJobs[job.WatchUid()]
		// job create
		if !ok {
			w.eventBus(jobEvent{
				opt: CREATE,
				job: job,
			})
			return
		}
		// job exist
		if name == existJob.filename {
			return
		}
		// FD is in hold, ignore
		if existJob.file != nil {
			return
		}
		// check existJob renamed?
		if existJob.IsSame(job) {
			w.eventBus(jobEvent{
				opt:         RENAME,
				job:         existJob,
				newFilename: name,
			})
			return
		}
	}
}

func (w *Watcher) legalFile(filename string, watchTask *WatchTask, withIgnoreOlder bool) (bool, string, os.FileInfo) {
	if ignoreSystemFile(filename) {
		return false, "", nil
	}
	pipelineName := watchTask.pipelineName
	sourceName := watchTask.sourceName

	filename, err := filepath.Abs(filename)
	if err != nil {
		log.Error("[pipeline(%s)-source(%s)]: get abs fileName(%s) error: %v", pipelineName, sourceName, filename, err)
		return false, "", nil
	}

	if watchTask.config.IsFileExcluded(filename) {
		log.Debug("[pipeline(%s)-source(%s)]: exclude fileName: %s", pipelineName, sourceName, filename)
		return false, "", nil
	}

	stat, err := os.Stat(filename)
	if err != nil {
		log.Error("[pipeline(%s)-source(%s)]: stat fileName(%s) fail: %v", pipelineName, sourceName, filename, err)
		return false, "", nil
	}

	if stat.IsDir() {
		log.Debug("[pipeline(%s)-source(%s)]: skip directory(%s)", pipelineName, sourceName, filename)
		return false, "", nil
	}

	// Fetch Lstat File info to detected also symlinks
	lstat, err := os.Lstat(filename)
	if err != nil {
		log.Warn("[pipeline(%s)-source(%s)]: lstat fileName(%s) fail: %v", pipelineName, sourceName, filename, err)
		return false, "", nil
	}

	isSymlink := lstat.Mode()&os.ModeSymlink != 0
	if isSymlink && watchTask.config.IgnoreSymlink {
		log.Info("[pipeline(%s)-source(%s)]: fileName(%s) skipped as it is a symlink", pipelineName, sourceName, filename)
		return false, "", nil
	}

	// Ignores all files which fall under ignore_older
	if withIgnoreOlder && watchTask.config.IsIgnoreOlder(stat) {
		log.Debug("[pipeline(%s)-source(%s)]: ignore file(%s) because ignore_older(%d second) reached", pipelineName, sourceName, filename, watchTask.config.IgnoreOlder.Duration()/time.Second)
		return false, "", nil
	}
	return true, filename, stat
}

func (w *Watcher) scan() {
	start := time.Now()

	// active job
	w.scanActiveJob()
	// check any new files
	w.scanNewFiles()
	// zombie job
	w.scanZombieJob()

	scanCost := time.Since(start)
	if scanCost > 3*time.Second {
		log.Warn("watch scan cost: %ds", scanCost/time.Second)
	}
}

func (w *Watcher) scanActiveJob() {
	for _, job := range w.allJobs {
		if job.IsStop() || w.isZombieJob(job) {
			continue
		}
		// check FdHoldTimeoutWhenRemove
		if job.IsDeleteTimeout(job.task.config.FdHoldTimeoutWhenRemove) {
			job.Stop()
			log.Info("[pipeline(%s)-source(%s)]: job stop because file(%s) fdHoldTimeoutWhenRemove(%d second) reached", job.task.pipelineName, job.task.sourceName, job.filename, job.task.config.FdHoldTimeoutWhenRemove/time.Second)
			continue
		}
		// check FdHoldTimeoutWhenInactive
		if time.Since(job.LastActiveTime()) > job.task.config.FdHoldTimeoutWhenInactive {
			job.Stop()
			log.Info("[pipeline(%s)-source(%s)]: job stop because file(%s) fdHoldTimeoutWhenInactive(%d second) reached", job.task.pipelineName, job.task.sourceName, job.filename, job.task.config.FdHoldTimeoutWhenInactive/time.Second)
			// more aggressive releasing of fd to prevent excessive memory usage
			if job.Release() {
				w.currentOpenFds--
			}
			continue
		}
	}
}

// check zombie job
func (w *Watcher) scanZombieJob() {
	for _, job := range w.zombieJobs {
		if job.IsDelete() {
			w.finalizeJob(job)
			continue
		}
		filename := job.filename
		stat, err := os.Stat(filename)
		var checkRemove = func() bool {
			if err != nil {
				if os.IsNotExist(err) {
					w.eventBus(jobEvent{
						opt: REMOVE,
						job: job,
					})
					return true
				}
				log.Error("stat file(%s) fail: %v", filename, err)
			}
			// check whether jobUid change
			newJobUid := JobUid(filename, stat)
			if newJobUid != job.Uid() {
				log.Debug("remove job(filename: %s) because jobUid changed: oldUid(%s) -> newUid(%s)", job.filename, job.Uid(), newJobUid)
				w.eventBus(jobEvent{
					opt: REMOVE,
					job: job,
				})
				return true
			}
			return false
		}

		if job.IsStop() {
			if checkRemove() {
				continue
			}
			w.finalizeJob(job)
			continue
		}

		if job.file == nil {
			// check remove
			if checkRemove() {
				continue
			}
			// check whether file was reduced
			size := stat.Size()
			currentOffset := job.endOffset
			if size < job.endOffset {
				log.Warn("job(jobUid: %s) file(%s) size was reduced: file size(%d) should greater than current offset(%d)", job.Uid(), filename, size, currentOffset)
				// Read from the beginning when the file is truncated
				if job.task.config.RereadTruncated {
					job.endOffset = 0
					job.nextOffset = 0
					job.currentLineNumber = 0
					w.eventBus(jobEvent{
						opt:         WRITE,
						job:         job,
						newFileSize: size,
					})
					continue
				}
			}
		} else {
			// release fd
			if time.Since(job.LastActiveTime()) > job.task.config.FdHoldTimeoutWhenInactive {
				if job.Release() {
					w.currentOpenFds--
				}
				if job.IsRename() {
					w.handleRenameJobs(job)
				}
				continue
			}
			stat, err = os.Stat(filename)
			if err != nil {
				// waiting for job release
				continue
			}
		}
		// any written?
		size := stat.Size()
		if size > job.nextOffset && !job.task.config.IsIgnoreOlder(stat) {
			w.eventBus(jobEvent{
				opt:         WRITE,
				job:         job,
				newFileSize: size,
			})
			continue
		}
	}
}

func (w *Watcher) finalizeJob(job *Job) {
	log.Info("finalize job(filename: %s)", job.filename)
	key := job.WatchUid()
	delete(w.zombieJobs, key)
	delete(w.allJobs, key)
	w.removeOsNotify(job.filename)
	if job.Release() {
		w.currentOpenFds--
	}

	if job.IsRename() {
		w.handleRenameJobs(job)
	}

	if job.IsDelete() {
		w.handleRemoveJobs(job)
	}

	for k, task := range w.waiteForStopWatchTasks {
		delete(task.waiteForStopJobs, job.WatchUid())
		if len(task.waiteForStopJobs) == 0 {
			task.waiteForStopJobs = nil
			delete(w.waiteForStopWatchTasks, k)
			task.countDown.Done()
		}
	}
}

func (w *Watcher) isZombieJob(job *Job) bool {
	_, ok := w.zombieJobs[job.WatchUid()]
	return ok
}

func (w *Watcher) run() {
	w.countDown.Add(1)
	log.Info("file watcher start")
	scanFileTicker := time.NewTicker(w.config.ScanTimeInterval)
	maintenanceTicker := time.NewTicker(w.config.MaintenanceInterval)
	defer func() {
		w.countDown.Done()
		scanFileTicker.Stop()
		maintenanceTicker.Stop()
		log.Info("file watcher stop")
	}()
	var osEvents chan fsnotify.Event
	if w.config.EnableOsWatch && w.osWatcher != nil {
		osEvents = w.osWatcher.Events
	}
	for {
		select {
		case <-w.done:
			return
		case watchTaskEvent := <-w.watchTaskEventChan:
			if watchTaskEvent.watchTaskType == START {
				w.scanNewFiles()
			}
			w.handleWatchTaskEvent(watchTaskEvent)
		case job := <-w.zombieJobChan:
			w.decideZombieJob(job)
		case e := <-osEvents:
			w.osNotify(e)
		case <-scanFileTicker.C:
			w.scan()
		case <-maintenanceTicker.C:
			w.maintenance()
		}
	}
}

func (w *Watcher) handleWatchTaskEvent(watchTaskEvent WatchTaskEvent) {
	taskType := watchTaskEvent.watchTaskType
	watchTask := watchTaskEvent.watchTask
	key := watchTask.WatchTaskKey()
	if taskType == START {
		// WatchTask may be stopped at the moment of starting
		if watchTask.IsStop() {
			return
		}
		w.sourceWatchTasks[key] = watchTask
		w.cleanWatchTaskRegistry(watchTask)
		return
	}
	if taskType == STOP {
		log.Info("try to stop watch task: %s", watchTask.String())
		delete(w.sourceWatchTasks, key)
		// Delete the jobs of the corresponding source
		waitForStopJobs := make(map[string]*Job)
		for _, job := range w.allJobs {
			if watchTask.isParentOf(job) {
				job.Stop()
				if w.isZombieJob(job) {
					w.finalizeJob(job)
					continue
				}
				waitForStopJobs[job.WatchUid()] = job
			}
		}
		if len(waitForStopJobs) > 0 {
			watchTask.waiteForStopJobs = waitForStopJobs
			w.waiteForStopWatchTasks[watchTask.WatchTaskKey()] = watchTask

			// Try to stop jobs more aggressively
			w.aggressivelyStopWatchTask(watchTask)
		} else {
			watchTask.countDown.Done()
		}
		return
	}
}

func (w *Watcher) aggressivelyStopWatchTask(watchTask *WatchTask) {
	go w.asyncStopTaskJobs(watchTask)

	if len(w.zombieJobChan) > 0 {
		for j := range w.zombieJobChan {
			w.decideZombieJob(j)
			if len(w.zombieJobChan) == 0 {
				break
			}
		}
	}
}

func (w *Watcher) asyncStopTaskJobs(watchTask *WatchTask) {
	timeout := time.NewTimer(w.config.CleanDataTimeout)
	defer timeout.Stop()

	for {
		select {
		case <-w.done:
			return
		case <-timeout.C:
			return
		case j := <-watchTask.activeChan:
			w.DecideJob(j)
		}
	}
}

func (w *Watcher) decideZombieJob(job *Job) {
	watchJobId := job.WatchUid()
	if !w.isZombieJob(job) {
		w.zombieJobs[watchJobId] = job
		w.addOsNotify(job.filename)
	}
}

func (w *Watcher) osNotify(e fsnotify.Event) {
	if e.Op == fsnotify.Chmod {
		// File writing will also be received. Ignore it. Only check whether you have read permission when the file job is activated (job. Active())
		return
	}

	if e.Op == fsnotify.Create {
		// return directly, because OS notify should only care about to write and remove of the file
		return
	}

	// Ignore the rename event, because the rename event must be accompanied by the create event.
	// The create event will determine whether the name is changed.
	// Moreover, the rename event carries the file name before renaming, so we can't know what the file name is changed to
	if e.Op == fsnotify.Rename {
		return
	}
	log.Debug("received os notify: %+v", e)

	fileName := e.Name
	if ignoreSystemFile(fileName) {
		return
	}

	fileName, err := filepath.Abs(fileName)
	if err != nil {
		log.Error("get abs fileName(%s) error: %v", fileName, err)
		return
	}

	if e.Op == fsnotify.Remove {
		for _, job := range w.allJobs {
			if job.IsDelete() || job.IsStop() {
				continue
			}
			if fileName == job.filename {
				w.eventBus(jobEvent{
					opt: REMOVE,
					job: job,
				})
			}
		}
		return
	}

	if e.Op == fsnotify.Write {
		stat, err := os.Stat(fileName)
		if err != nil {
			log.Warn("os notify stat file(%s) fail: %s", fileName, err)
			return
		}
		jobUid := JobUid(fileName, stat)
		for _, existJob := range w.allJobs {
			if existJob.Uid() == jobUid {
				w.eventBus(jobEvent{
					opt:         WRITE,
					job:         existJob,
					newFileSize: stat.Size(),
				})
			}
		}
	}
}

func (w *Watcher) maintenance() {
	w.reportWatchMetricAndCleanFiles()
	w.checkWaitForStopTask()
}

func (w *Watcher) checkWaitForStopTask() {
	if len(w.waiteForStopWatchTasks) <= 0 {
		return
	}
	for _, watchTask := range w.waiteForStopWatchTasks {
		if time.Since(watchTask.stopTime) > w.config.TaskStopTimeout {
			log.Error("watchTask(%s) stop timeout because jobs has not release: %s", watchTask.String(), watchTask.StopJobsInfo())
			for _, job := range watchTask.waiteForStopJobs {
				existJob, exist := w.allJobs[job.WatchUid()]
				if exist {
					log.Warn("job(%s:%s) exist, status: %d", existJob.WatchUid(), existJob.filename, existJob.status)
				} else {
					log.Warn("job(%s:%s) was deleted but not finalize", job.WatchUid(), job.filename)
				}
				job.Stop()
				w.finalizeJob(job)
			}
		}
	}
}

func (w *Watcher) reportWatchMetricAndCleanFiles() {
	for _, watchTask := range w.sourceWatchTasks {
		pipelineName := watchTask.pipelineName
		sourceName := watchTask.sourceName

		paths := getPathsIfDynamicContainerLogs(watchTask.config.Paths, pipelineName, sourceName)
		watchMetricData := w.reportWatchMetric(watchTask, paths, pipelineName, sourceName)
		eventbus.PublishOrDrop(eventbus.FileWatcherTopic, watchMetricData)

		removedFiles := w.cleanFiles(watchTask, watchMetricData.FileInfos)
		if len(removedFiles) > 0 {
			log.Info("cleanLogs: removed files %+v", removedFiles)
		}
	}
}

func (w *Watcher) reportWatchMetric(watchTask *WatchTask, paths []string, pipelineName string, sourceName string) eventbus.WatchMetricData {
	var (
		activeFdCount   int
		inActiveFdCount int
	)
	for _, job := range w.allJobs {
		if job.file == nil {
			continue
		}
		if job.task.pipelineName != watchTask.pipelineName {
			continue
		}
		if job.task.sourceName != watchTask.sourceName {
			continue
		}
		if j, ok := w.zombieJobs[job.WatchUid()]; ok {
			if j.file != nil {
				inActiveFdCount++
			}
		} else {
			activeFdCount++
		}
	}

	fileInfos := make([]eventbus.FileInfo, 0)
	for _, path := range paths {
		matches, err := util.GlobWithRecursive(path)
		if err != nil {
			log.Info("[pipeline(%s)-source(%s)]: glob path(%s) fail: %v", pipelineName, sourceName, path, err)
			continue
		}
		for _, fileName := range matches {
			if legal, f, stat := w.legalFile(fileName, watchTask, false); legal {
				job := watchTask.newJob(f, stat)
				existRegistry := w.findExistJobRegistry(job)
				existOffset := existRegistry.Offset
				existJob, exist := w.allJobs[job.WatchUid()]
				fileInfo := eventbus.FileInfo{
					FileName:       f,
					Size:           stat.Size(),
					LastModifyTime: stat.ModTime(),
					Offset:         existOffset,
					IsIgnoreOlder:  job.task.config.IsIgnoreOlder(stat),
					IsRelease:      exist && existJob.file == nil,
				}
				fileInfos = append(fileInfos, fileInfo)
			}
		}
	}

	watchMetricData := eventbus.WatchMetricData{
		BaseMetric: eventbus.BaseMetric{
			PipelineName: pipelineName,
			SourceName:   sourceName,
		},
		Paths:           paths,
		FileInfos:       fileInfos,
		ActiveFileCount: activeFdCount,
		InactiveFdCount: inActiveFdCount,
		SourceFields:    watchTask.sourceFields,
	}

	return watchMetricData
}

// ExportWatchMetric export all pipeline/source files info
func ExportWatchMetric() map[string]eventbus.WatchMetricData {
	watcherMetrics := make(map[string]eventbus.WatchMetricData)

	watchLock.Lock()
	defer watchLock.Unlock()
	if globalWatcher == nil {
		return watcherMetrics
	}
	for _, watchTask := range globalWatcher.sourceWatchTasks {
		paths := getPathsIfDynamicContainerLogs(watchTask.config.Paths, watchTask.pipelineName, watchTask.sourceName)
		m := globalWatcher.reportWatchMetric(watchTask, paths, watchTask.pipelineName, watchTask.sourceName)
		watcherMetrics[fmt.Sprintf("%s/%s", watchTask.pipelineName, watchTask.sourceName)] = m
	}

	return watcherMetrics
}

func (w *Watcher) cleanFiles(watchTask *WatchTask, infos []eventbus.FileInfo) []string {
	if watchTask == nil {
		return nil
	}
	if watchTask.config.CleanFiles == nil {
		return nil
	}

	var maxHistoryDays int
	if watchTask.config.CleanFiles != nil {
		maxHistoryDays = watchTask.config.CleanFiles.MaxHistoryDays
	}

	history, err := time.ParseDuration(fmt.Sprintf("%dh", maxHistoryDays*24))
	if err != nil {
		log.Warn("parse duration of cleanLogs.maxHistoryDays error: %v", err)
		return nil
	}

	var fileRemoved []string
	for _, info := range infos {
		if maxHistoryDays > 0 {
			if time.Since(info.LastModifyTime) < history {
				continue
			}

			// if file is not finished, do not remove it
			if watchTask.config.CleanFiles != nil {
				if !watchTask.config.CleanFiles.CleanUnfinished && info.Offset < info.Size {
					continue
				}
			}

			_ = truncateAndRemoveFile(info.FileName)
			fileRemoved = append(fileRemoved, info.FileName)
		}
	}

	return fileRemoved
}

func truncateAndRemoveFile(filepath string) error {
	if err := os.Truncate(filepath, 0); err != nil {
		log.Warn("truncate file %s error: %+v", filepath, err)
		return err
	}

	if err := os.Remove(filepath); err != nil {
		log.Warn("remove file %s error: %+v", filepath, err)
		return err
	}

	return nil
}

func (w *Watcher) handleRenameJobs(jobs ...*Job) {
	l := len(jobs)
	if l == 0 {
		return
	}
	for _, job := range jobs {
		jt := job
		r := reg.Registry{
			PipelineName: jt.task.pipelineName,
			SourceName:   jt.task.sourceName,
			Filename:     jt.filename,
			JobUid:       jt.Uid(),
		}
		log.Info("try to rename job: %s", job.filename)
		w.dbHandler.HandleOpt(persistence.DbOpt{
			R:           r,
			OptType:     persistence.UpdateNameByJobWatchIdOpt,
			Immediately: false,
		})
		job.cleanRename()
	}
}

func (w *Watcher) handleRemoveJobs(jobs ...*Job) {
	if !w.config.CleanWhenRemoved {
		return
	}
	l := len(jobs)
	if l == 0 {
		return
	}
	for _, j := range jobs {
		jt := j
		r := reg.Registry{
			PipelineName: jt.task.pipelineName,
			SourceName:   jt.task.sourceName,
			JobUid:       jt.Uid(),
			Filename:     jt.filename,
		}
		log.Info("try to delete registry(%+v). deleteTime: %s", r, jt.deleteTime.Load().(time.Time).Format(persistence.TimeFormatPattern))
		w.dbHandler.HandleOpt(persistence.DbOpt{
			R:           r,
			OptType:     persistence.DeleteByJobUidOpt,
			Immediately: false,
		})
	}
}
