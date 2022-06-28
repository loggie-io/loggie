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
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util"
)

const (
	JobActive = JobStatus(1)
	JobDelete = JobStatus(2)
	JobStop   = JobStatus(3)
)

var NilOfTime, _ = time.ParseInLocation("2006-01-02 15:04:05", "2008-08-08 08:08:08", time.Local)

var globalJobIndex uint32

type JobStatus int

type Job struct {
	uid               string
	watchUid          string
	watchUidLen       int
	index             uint32
	filename          string
	aFileName         atomic.Value
	file              *os.File
	status            JobStatus
	aStatus           atomic.Value
	endOffset         int64
	nextOffset        int64
	currentLineNumber int64
	currentLines      int64
	deleteTime        atomic.Value
	renameTime        atomic.Value
	stopTime          atomic.Value
	identifier        string

	task *WatchTask

	EofCount       int
	LastActiveTime time.Time

	lineEnd       []byte
	encodeLineEnd []byte
}

func JobUid(fileInfo os.FileInfo) string {
	stat := fileInfo.Sys().(*syscall.Stat_t)
	inode := stat.Ino
	device := stat.Dev
	var buf [64]byte
	current := strconv.AppendUint(buf[:0], inode, 10)
	current = append(current, '-')
	current = strconv.AppendUint(current, uint64(device), 10)
	return string(current)
}

func WatchJobId(pipelineName string, sourceName string, jobUid string) string {
	var watchJobId strings.Builder
	watchJobId.WriteString(pipelineName)
	watchJobId.WriteString(":")
	watchJobId.WriteString(sourceName)
	watchJobId.WriteString(":")
	watchJobId.WriteString(jobUid)
	return watchJobId.String()
}

// WatchUid Support repeated collection of the same file by different sources
func (j *Job) WatchUid() string {
	if j.watchUid != "" {
		return j.watchUid
	}
	wj := WatchJobId(j.task.pipelineName, j.task.sourceName, j.Uid())
	j.watchUid = wj
	j.watchUidLen = len(j.watchUid)
	return wj
}

func (j *Job) Uid() string {
	return j.uid
}
func (j *Job) Index() uint32 {
	return j.index
}

func (j *Job) Delete() {
	j.ChangeStatusTo(JobDelete)
	j.deleteTime.Store(time.Now())
}

func (j *Job) IsDelete() bool {
	if j.status == JobDelete {
		return true
	}
	dt := j.deleteTime.Load()
	if dt == nil {
		return false
	}
	return !dt.(time.Time).IsZero()
}

func (j *Job) IsDeleteTimeout(timeout time.Duration) bool {
	if !j.IsDelete() {
		return false
	}
	return time.Since(j.deleteTime.Load().(time.Time)) > timeout
}

func (j *Job) Stop() {
	j.ChangeStatusTo(JobStop)
	j.stopTime.Store(time.Now())
}

func (j *Job) IsStop() bool {
	if j.status == JobStop {
		return true
	}
	st := j.stopTime.Load()
	if st == nil {
		return false
	}
	return !st.(time.Time).IsZero()
}

func (j *Job) ChangeStatusTo(status JobStatus) {
	j.status = status
	j.aStatus.Store(status)
}

func (j *Job) Release() bool {
	if j.file == nil {
		return false
	}
	err := j.file.Close()
	if err != nil {
		log.Error("release job(fileName: %s) error: %s", j.filename, err)
	}
	j.file = nil
	log.Info("job(fileName: %s) has been released", j.filename)
	return true
}

func (j *Job) Sync() {
	j.status = j.aStatus.Load().(JobStatus)
	j.filename = j.aFileName.Load().(string)
}

func (j *Job) RenameTo(newFilename string) {
	j.filename = newFilename
	j.aFileName.Store(newFilename)
	j.renameTime.Store(time.Now())
}

func (j *Job) IsRename() bool {
	rt := j.renameTime.Load()
	if rt == nil || rt == NilOfTime {
		return false
	}
	return !rt.(time.Time).IsZero()
}

func (j *Job) cleanRename() {
	j.renameTime.Store(NilOfTime)
}

func (j *Job) Active() (error, bool) {
	fdOpen := false
	if j.file == nil {
		// reopen
		file, err := os.Open(j.filename)
		if err != nil {
			if os.IsPermission(err) {
				log.Error("no permission for filename: %s", j.filename)
			}
			return err, false
		}
		j.file = file
		fdOpen = true

		fileInfo, err := file.Stat()
		if err != nil {
			return err, fdOpen
		}
		newUid := JobUid(fileInfo)
		if j.Uid() != newUid {
			j.Delete()
			return fmt.Errorf("job(filename: %s) uid(%s) changed to %s，it maybe not a file", j.filename, j.Uid(), newUid), fdOpen
		}

		// reset file offset and lineNumber
		if j.nextOffset != 0 {
			_, err = file.Seek(j.nextOffset, io.SeekStart)
			if err != nil {
				return err, fdOpen
			}
			// init lineNumber
			if j.currentLineNumber == 0 {
				lineNumber, err := util.LineCountTo(j.nextOffset, j.filename)
				if err != nil {
					return err, fdOpen
				}
				j.currentLineNumber = int64(lineNumber)
			}
		}
	}
	j.ChangeStatusTo(JobActive)
	j.EofCount = 0
	j.LastActiveTime = time.Now()
	return nil, fdOpen
}

func (j *Job) NextOffset(offset int64) {
	if offset > 0 {
		j.nextOffset = offset
	}
}

func (j *Job) GenerateIdentifier() error {
	if j.identifier != "" {
		return nil
	}
	stat, err := os.Stat(j.filename)
	if err != nil {
		return err
	}
	readSize := j.task.config.FirstNBytesForIdentifier
	fileSize := stat.Size()
	if fileSize < int64(readSize) {
		return fmt.Errorf("file size is smaller than firstNBytesForIdentifier: %d < %d", fileSize, readSize)
	}
	file, err := os.Open(j.filename)
	if err != nil {
		return err
	}
	defer file.Close()
	readBuffer := make([]byte, readSize)
	l, err := file.Read(readBuffer)
	if err != nil {
		return err
	}
	if l < readSize {
		return fmt.Errorf("read size is smaller than firstNBytesForIdentifier: %d < %d", l, readSize)
	}
	j.identifier = fmt.Sprintf("%x", md5.Sum(readBuffer))
	return nil
}

func (j *Job) IsSame(other *Job) bool {
	if other == nil {
		return false
	}
	if j == other {
		return true
	}
	if j.WatchUid() != other.WatchUid() {
		return false
	}
	return j.identifier == other.identifier
}

func (j *Job) Read() {
	j.task.activeChan <- j
}

func (j *Job) File() *os.File {
	return j.file
}

const tsLayout = "2006-01-02T15:04:05.000Z"

func (j *Job) GetEncodeLineEnd() []byte {
	return j.lineEnd
}

func (j *Job) GetLineEnd() []byte {
	return j.encodeLineEnd
}

func (j *Job) ProductEvent(endOffset int64, collectTime time.Time, body []byte) {
	nextOffset := endOffset + int64(len(j.GetEncodeLineEnd()))
	contentBytes := int64(len(body))
	// -1 because `\n`
	startOffset := nextOffset - contentBytes - int64(len(j.GetEncodeLineEnd()))

	j.currentLineNumber++
	j.currentLines++
	j.endOffset = endOffset
	j.nextOffset = nextOffset
	watchUid := j.WatchUid()

	endOffsetStr := strconv.FormatInt(endOffset, 10)
	var eventUid strings.Builder
	eventUid.Grow(j.watchUidLen + len(j.GetEncodeLineEnd()) + len(endOffsetStr))
	eventUid.WriteString(watchUid)
	eventUid.WriteString("-")
	eventUid.WriteString(endOffsetStr)
	state := &State{
		Epoch:        j.task.epoch,
		PipelineName: j.task.pipelineName,
		SourceName:   j.task.sourceName,
		Offset:       startOffset,
		NextOffset:   nextOffset,
		LineNumber:   j.currentLineNumber,
		Filename:     j.filename,
		CollectTime:  collectTime,
		ContentBytes: contentBytes + int64(len(j.GetEncodeLineEnd())), // because `\n`
		JobUid:       j.Uid(),
		JobIndex:     j.Index(),
		watchUid:     watchUid,
		EventUid:     eventUid.String(),
	}
	e := j.task.eventPool.Get()
	e.Meta().Set(SystemStateKey, state)
	// copy body,because readBuffer reuse
	contentBuffer := make([]byte, contentBytes)
	copy(contentBuffer, body)
	e.Fill(e.Meta(), e.Header(), contentBuffer)
	j.task.productFunc(e)
}

func NewJob(task *WatchTask, filename string, fileInfo os.FileInfo) *Job {
	jobUid := JobUid(fileInfo)
	return newJobWithUid(task, filename, jobUid)
}

func newJobWithUid(task *WatchTask, filename string, jobUid string) *Job {
	j := &Job{
		task:          task,
		index:         jobIndex(),
		filename:      filename,
		uid:           jobUid,
		lineEnd:       globalLineEnd.GetLineEnd(task.pipelineName, task.sourceName),
		encodeLineEnd: globalLineEnd.GetEncodeLineEnd(task.pipelineName, task.sourceName),
	}
	j.aFileName.Store(filename)
	return j
}

func jobIndex() uint32 {
	return atomic.AddUint32(&globalJobIndex, 1)
}
