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
	"github.com/fsnotify/fsnotify"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/eventbus"
	"loggie.io/loggie/pkg/util"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	CREATE  = Operation(0)
	WRITE   = Operation(1)
	REMOVE  = Operation(2)
	RENAME  = Operation(3)
	CHMOD   = Operation(4)
	RELEASE = Operation(5)
)

type Operation int

type jobEvent struct {
	opt         Operation
	job         *Job
	newFilename string
}

type Watcher struct {
	done                   chan struct{}
	config                 WatchConfig
	sourceWatchTasks       map[string]*WatchTask // key:pipelineName:sourceName
	waiteForStopWatchTasks map[string]*WatchTask
	watchTaskChan          chan *WatchTask
	osWatcher              *fsnotify.Watcher
	osWatchFiles           map[string]bool // key:file|value:1;only zombie job file need os notify
	allJobs                map[string]*Job // key:`pipelineName:sourceName:job.Uid`|value:*job
	zombieJobs             map[string]*Job // key:`pipelineName:sourceName:job.Uid`|value:*job
	currentOpenFds         int
	zombieJobChan          chan *Job
	dbHandler              *dbHandler
	countDown              *sync.WaitGroup
	stopOnce               *sync.Once
}

func newWatcher(config WatchConfig, dbHandler *dbHandler) *Watcher {
	w := &Watcher{
		done:                   make(chan struct{}),
		config:                 config,
		sourceWatchTasks:       make(map[string]*WatchTask),
		waiteForStopWatchTasks: make(map[string]*WatchTask),
		watchTaskChan:          make(chan *WatchTask),
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
	watchTask.watchTaskType = STOP
	watchTask.countDown.Add(1)
	w.watchTaskChan <- watchTask
	watchTask.countDown.Wait()
}

func (w *Watcher) StartWatchTask(watchTask *WatchTask) {
	watchTask.watchTaskType = START
	w.watchTaskChan <- watchTask
}

func (w *Watcher) preAllocationOffset(size int64, job *Job) {
	w.dbHandler.HandleOpt(DbOpt{
		r: registry{
			PipelineName: job.task.pipelineName,
			SourceName:   job.task.sourceName,
			Filename:     job.filename,
			JobUid:       job.Uid(),
			Offset:       size,
		},
		optType:     UpsertOffsetByJobWatchIdOpt,
		immediately: true,
	})
}

func (w *Watcher) findExistJobRegistry(job *Job) registry {
	return w.dbHandler.findBy(job.Uid(), job.task.sourceName, job.task.pipelineName)
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
			log.Warn("remove file(%s) os notify fail: %v", file, err)
		}
	}
}

// The operations here should be as lightweight as possible.
// Operations such as releasing file handles should be placed in a separate goroutine of watch
func (w *Watcher) decideJob(job *Job) {
	job.Sync()

	w.reportMetric(job)

	// Stopped jobs are directly put into the zombie queue for release
	if job.status == JobStop || job.status == JobStopImmediately {
		w.zombieJobChan <- job
		return
	}
	// inactive
	if job.eofCount > w.config.MaxEofCount {
		w.zombieJobChan <- job
		return
	}
	//w.activeChan <- job
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
		FileName:   job.filename,
		Offset:     job.endOffset,
		LineNumber: job.currentLineNumber,
		Lines:      job.currentLines,
		//FileSize:   fileSize,
	}
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
		// only care about zombie job write event
		watchJobId := job.WatchUid()
		if job, ok := w.zombieJobs[watchJobId]; ok {
			delete(w.zombieJobs, watchJobId)
			// zombie job change to active, so without os notify
			w.removeOsNotify(job.filename)
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
			job.Read()
		}
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
		fileSize := stat.Size()
		// check whether the existAckOffset is larger than the file size
		if existAckOffset > fileSize {
			log.Warn("new job(jobUid:%s) fileName(%s) existRegistry(%+v) ackOffset is larger than file size(%d).is inode repeat?", job.Uid(), filename, existRegistry, fileSize)
			// file was truncated，start from the beginning
			if job.task.config.RereadTruncated {
				existAckOffset = 0
			}
		}
		// PreAllocationOffsetWithSize
		if existAckOffset == 0 && w.config.ReadFromTail {
			w.preAllocationOffset(fileSize, job)
			existAckOffset = fileSize
		}
		// set ack offset
		job.NextOffset(existAckOffset)
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
	registries := w.dbHandler.findAll()
	for _, r := range registries {
		if r.PipelineName == watchTask.pipelineName && r.SourceName == watchTask.sourceName {
			// file remove?
			_, err := os.Stat(r.Filename)
			if err != nil && os.IsNotExist(err) {
				// delete registry
				w.dbHandler.HandleOpt(DbOpt{
					r:           r,
					optType:     DeleteByIdOpt,
					immediately: true,
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
	for _, path := range paths {
		matches, err := util.GlobWithRecursive(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			log.Info("[pipeline(%s)-source(%s)]: glob path(%s) fail: %v", pipelineName, sourceName, path, err)
			continue
		}
		for _, fileName := range matches {
			w.createOrRename(fileName, watchTask)
		}
	}
}

func (w *Watcher) createOrRename(filename string, watchTask *WatchTask) {
	if legal, filename, fileInfo := w.legalFile(filename, watchTask, true); legal {
		job := watchTask.newJob(filename, fileInfo)
		err := job.GenerateIdentifier()
		if err != nil {
			log.Info("file(%s) ignored because generate id fail: %s", filename, err)
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
		if filename == existJob.filename {
			return
		}
		// FD is in hold, ignore
		if existJob.file != nil {
			return
		}
		// check existJob renamed?
		if existJob.IsSame(job) && existJob.endOffset <= fileInfo.Size() {
			w.eventBus(jobEvent{
				opt:         RENAME,
				job:         existJob,
				newFilename: filename,
			})
			return
		}
		// check existJob removed?
		_, err = os.Stat(existJob.filename)
		if err != nil && os.IsNotExist(err) {
			w.eventBus(jobEvent{
				opt:         REMOVE,
				job:         existJob,
				newFilename: filename,
			})
		} else {
			log.Error("files has same inode and identifier: fileName(%s) and fileName(%s) inode(%s) repeat!", existJob.filename, filename, existJob.Uid())
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

	if !watchTask.config.IsFileInclude(filename) {
		log.Debug("[pipeline(%s)-source(%s)]: not include fileName: %s", pipelineName, sourceName, filename)
		return false, "", nil
	}

	if watchTask.config.IsFileExcluded(filename) {
		log.Debug("[pipeline(%s)-source(%s)]: exclude fileName: %s", pipelineName, sourceName, filename)
		return false, "", nil
	}

	// Fetch Lstat File info to detected also symlinks
	fileInfo, err := os.Lstat(filename)
	if err != nil {
		log.Warn("[pipeline(%s)-source(%s)]: lstat fileName(%s) fail: %v", pipelineName, sourceName, filename, err)
		return false, "", nil
	}

	if fileInfo.IsDir() {
		log.Debug("[pipeline(%s)-source(%s)]: skip directory(%s)", pipelineName, sourceName, filename)
		return false, "", nil
	}

	isSymlink := fileInfo.Mode()&os.ModeSymlink != 0
	if isSymlink {
		// TODO support symlink
		log.Info("[pipeline(%s)-source(%s)]: fileName(%s) skipped as it is a symlink", pipelineName, sourceName, filename)
		return false, "", nil
	}

	fileInfo, err = os.Stat(filename)
	if err != nil {
		log.Error("[pipeline(%s)-source(%s)]: stat fileName(%s) fail: %v", pipelineName, sourceName, filename, err)
		return false, "", nil
	}

	// Ignores all files which fall under ignore_older
	if withIgnoreOlder && watchTask.config.IsIgnoreOlder(fileInfo) {
		log.Debug("[pipeline(%s)-source(%s)]: ignore file(%s) because ignore_older(%d second) reached", pipelineName, sourceName, filename, watchTask.config.IgnoreOlder/time.Second)
		return false, "", nil
	}
	return true, filename, fileInfo
}

func (w *Watcher) scan() {
	//if w.isIgnoreTime() {
	//	return
	//}
	// check any new files
	w.scanNewFiles()
	// active job
	w.scanActiveJob()
	// zombie job
	w.scanZombieJob()
}

func (w *Watcher) isIgnoreTime() bool {
	ignoreScanTime := w.config.IgnoreScanTime
	if ignoreScanTime == 0 {
		return false
	}
	timeStr := time.Now().Format("2006-01-02")
	dayOfStart, _ := time.ParseInLocation("2006-01-02", timeStr, time.Local)
	return time.Since(dayOfStart) < ignoreScanTime
}

// 0. check remove fd
func (w *Watcher) scanActiveJob() {
	activeJobs := w.copyActiveJobs()
	fdHoldTimeoutWhenRemove := w.config.FdHoldTimeoutWhenRemove
	for _, job := range activeJobs {
		if job.status == JobStop || job.status == JobStopImmediately {
			continue
		}
		// check FdHoldTimeoutWhenRemove
		if job.IsDeleteTimeout(fdHoldTimeoutWhenRemove) {
			job.Stop()
			log.Info("[pipeline(%s)-source(%s)]: job stop because file(%s) fdHoldTimeoutWhenRemove(%d second) reached", job.task.pipelineName, job.task.sourceName, job.filename, fdHoldTimeoutWhenRemove/time.Second)
			continue
		}
	}
}

// check zombie job:
//  0. final status
//  1. remove
//  2. fd hold timeout,release fd
//  3. write
//  4. truncated file
func (w *Watcher) scanZombieJob() {
	for _, job := range w.zombieJobs {
		if job.IsDelete() {
			w.finalizeJob(job)
			continue
		}
		filename := job.filename
		var stat os.FileInfo
		var err error
		if job.file == nil {
			if job.status == JobStop || job.status == JobStopImmediately {
				w.finalizeJob(job)
				continue
			}
			// check remove
			stat, err = os.Stat(filename)
			if err != nil {
				if os.IsNotExist(err) {
					w.eventBus(jobEvent{
						opt: REMOVE,
						job: job,
					})
				} else {
					log.Error("stat file(%s) fail: %v", filename, err)
				}
				continue
			}
			// check whether jobUid change
			newJobUid := JobUid(stat)
			if newJobUid != job.Uid() {
				log.Debug("remove job(filename: %s) because jobUid changed: oldUid(%s) -> newUid(%s)", job.filename, job.Uid(), newJobUid)
				w.eventBus(jobEvent{
					opt: REMOVE,
					job: job,
				})
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
						opt: WRITE,
						job: job,
					})
					continue
				}
			}
		} else {
			if job.status == JobStopImmediately {
				if job.Release() {
					w.currentOpenFds--
				}
				if job.IsRename() {
					w.handleRenameJobs(job)
				}
				// waiting for next round process,because rename or remove decide
				continue
			}
			// release fd
			if time.Since(job.lastActiveTime) > w.config.FdHoldTimeoutWhenInactive {
				if job.Release() {
					w.currentOpenFds--
				}
				if job.IsRename() {
					w.handleRenameJobs(job)
				}
				continue
			}
			if job.status == JobStop {
				// wait for job release
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
				opt: WRITE,
				job: job,
			})
			continue
		}
	}
}

func (w *Watcher) finalizeJob(job *Job) {
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

	for _, task := range w.waiteForStopWatchTasks {
		if task.isParentOf(job) {
			task.waiteForStopJobCount--
			if task.waiteForStopJobCount == 0 {
				task.countDown.Done()
			}
		}
	}
}

func (w *Watcher) copyActiveJobs() []*Job {
	jobs := make([]*Job, 0)
	for _, job := range w.allJobs {
		if !w.isZombieJob(job) {
			jobs = append(jobs, job)
		}
	}
	return jobs
}

func (w *Watcher) isZombieJob(job *Job) bool {
	_, ok := w.zombieJobs[job.WatchUid()]
	return ok
}

func (w *Watcher) run() {
	w.countDown.Add(1)
	log.Info("watcher start")
	scanFileTicker := time.NewTicker(w.config.ScanTimeInterval)
	maintenanceTicker := time.NewTicker(w.config.MaintenanceInterval)
	defer func() {
		w.countDown.Done()
		scanFileTicker.Stop()
		maintenanceTicker.Stop()
		log.Info("watcher stop")
	}()
	var osEvents chan fsnotify.Event
	if w.config.EnableOsWatch && w.osWatcher != nil {
		osEvents = w.osWatcher.Events
	}
	for {
		select {
		case <-w.done:
			return
		case watchTask := <-w.watchTaskChan:
			key := watchTask.WatchTaskKey()
			if watchTask.watchTaskType == START {
				w.sourceWatchTasks[key] = watchTask
				w.cleanWatchTaskRegistry(watchTask)
			} else if watchTask.watchTaskType == STOP {
				delete(w.sourceWatchTasks, key)
				// Delete the jobs of the corresponding source
				for _, job := range w.allJobs {
					if watchTask.isParentOf(job) {
						job.ChangeStatusTo(JobStopImmediately)
					}
					if w.isZombieJob(job) {
						w.finalizeJob(job)
						continue
					}
					watchTask.waiteForStopJobCount++
				}
				if watchTask.waiteForStopJobCount > 0 {
					w.waiteForStopWatchTasks[watchTask.WatchTaskKey()] = watchTask
				} else {
					watchTask.countDown.Done()
				}
			}
		case job := <-w.zombieJobChan:
			watchJobId := job.WatchUid()
			if _, ok := w.zombieJobs[watchJobId]; !ok {
				w.zombieJobs[watchJobId] = job
				w.addOsNotify(job.filename)
			}
		case e := <-osEvents:
			w.osNotify(e)
		case <-scanFileTicker.C:
			w.scan()
		case <-maintenanceTicker.C:
			w.maintenance()
		}
	}
}

func (w *Watcher) osNotify(e fsnotify.Event) {
	log.Debug("received os notify: %+v", e)
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

	fileName := e.Name
	if ignoreSystemFile(fileName) {
		return
	}

	fileName, err := filepath.Abs(fileName)
	if err != nil {
		log.Error("get abs fileName(%s) error: %v", fileName, err)
		return
	}

	var opt Operation
	switch e.Op {
	case fsnotify.Remove:
		opt = REMOVE
	case fsnotify.Write:
		opt = WRITE
	}

	for _, job := range w.allJobs {
		if job.status == JobDelete || job.status == JobStop || job.status == JobStopImmediately {
			continue
		}
		if job.filename == fileName {
			w.eventBus(jobEvent{
				opt: opt,
				job: job,
			})
		}
	}
}

func (w *Watcher) maintenance() {
	w.reportWatchMetricAndCleanFiles()
}

func (w *Watcher) reportWatchMetricAndCleanFiles() {
	for _, watchTask := range w.sourceWatchTasks {
		pipelineName := watchTask.pipelineName
		sourceName := watchTask.sourceName
		paths := watchTask.config.Paths
		var (
			activeCount     int
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
				activeCount++
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
			FileInfos:       fileInfos,
			TotalFileCount:  activeCount,
			InactiveFdCount: inActiveFdCount,
		}
		eventbus.Publish(eventbus.FileWatcherTopic, watchMetricData)

		removedFiles := w.cleanFiles(fileInfos)
		if len(removedFiles) > 0 {
			log.Info("cleanLogs: removed files %+v", removedFiles)
		}
	}
}

func (w *Watcher) cleanFiles(infos []eventbus.FileInfo) []string {
	if w.config.CleanFiles == nil {
		return nil
	}

	history, err := time.ParseDuration(fmt.Sprintf("%dh", w.config.CleanFiles.MaxHistoryDays*24))
	if err != nil {
		log.Warn("parse duration of cleanLogs.maxHistoryDays error: %v", err)
		return nil
	}

	var fileRemoved []string
	for _, info := range infos {
		if w.config.CleanFiles.MaxHistoryDays > 0 {
			if time.Since(info.LastModifyTime) < history {
				continue
			}

			if info.Offset < info.Size {
				continue
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
	//rs := make([]registry, 0, l)
	for _, job := range jobs {
		jt := job
		r := registry{
			PipelineName: jt.task.pipelineName,
			SourceName:   jt.task.sourceName,
			Filename:     jt.filename,
			JobUid:       jt.Uid(),
		}
		//rs = append(rs, r)
		w.dbHandler.HandleOpt(DbOpt{
			r:           r,
			optType:     UpdateNameByJobWatchIdOpt,
			immediately: false,
		})
	}
	//w.dbHandler.updateName(rs)
}

func (w *Watcher) handleRemoveJobs(jobs ...*Job) {
	if !w.config.CleanWhenRemoved {
		return
	}
	l := len(jobs)
	if l == 0 {
		return
	}
	//rs := make([]registry, 0, l)
	for _, j := range jobs {
		jt := j
		r := registry{
			PipelineName: jt.task.pipelineName,
			SourceName:   jt.task.sourceName,
			JobUid:       jt.Uid(),
			Filename:     jt.filename,
		}
		//rs = append(rs, r)
		log.Info("try to delete registry(%+v) because CleanWhenRemoved. deleteTime: %s", r, jt.deleteTime.Load().(time.Time).Format(timeFormatPattern))
		w.dbHandler.HandleOpt(DbOpt{
			r:           r,
			optType:     DeleteByJobUidOpt,
			immediately: false,
		})
	}
	//w.dbHandler.deleteRemoved(rs)
}
