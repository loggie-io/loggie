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
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/pkg/errors"
)

const (
	SystemStateKey = event.SystemKeyPrefix + "State"
)

type State struct {
	Epoch        *pipeline.Epoch `json:"-"`
	PipelineName string          `json:"-"`
	SourceName   string          `json:"-"`
	Offset       int64           `json:"offset"`
	NextOffset   int64           `json:"nextOffset"`
	Filename     string          `json:"filename,omitempty"`
	CollectTime  time.Time       `json:"collectTime,omitempty"`
	ContentBytes int64           `json:"contentBytes"`
	JobUid       string          `json:"jobUid,omitempty"`
	JobIndex     uint32          `json:"-"`
	EventUid     string          `json:"-"`
	LineNumber   int64           `json:"lineNumber,omitempty"`
	Tags         string          `json:"tags,omitempty"`

	// for cache
	watchUid string
}

func (s *State) WatchUid() string {
	return s.watchUid
}

func (s *State) AppendTags(tag string) {
	if s.Tags == "" {
		s.Tags = tag
	} else {
		s.Tags = s.Tags + "," + tag
	}
}

type Reader struct {
	done      chan struct{}
	config    ReaderConfig
	jobChan   chan *Job
	watcher   *Watcher
	countDown *sync.WaitGroup
	stopOnce  *sync.Once
	startOnce *sync.Once
}

func newReader(config ReaderConfig, watcher *Watcher) *Reader {
	r := &Reader{
		done:      make(chan struct{}),
		config:    config,
		jobChan:   make(chan *Job, config.ReadChanSize),
		watcher:   watcher,
		countDown: &sync.WaitGroup{},
		stopOnce:  &sync.Once{},
		startOnce: &sync.Once{},
	}
	r.Start()
	return r
}

func (r *Reader) Stop() {
	r.stopOnce.Do(func() {
		close(r.done)
		r.countDown.Wait()
	})
}

func (r *Reader) Start() {
	r.startOnce.Do(func() {
		for i := 0; i < r.config.WorkerCount; i++ {
			index := i
			go r.work(index)
		}
	})
}

func (r *Reader) work(index int) {
	r.countDown.Add(1)
	log.Info("read worker-%d start", index)
	defer func() {
		log.Info("read worker-%d stop", index)
		r.countDown.Done()
	}()
	readBufferSize := r.config.ReadBufferSize
	maxContinueReadTimeout := r.config.MaxContinueReadTimeout
	maxContinueRead := r.config.MaxContinueRead
	inactiveTimeout := r.config.InactiveTimeout
	backlogBuffer := make([]byte, 0, readBufferSize)
	readBuffer := make([]byte, readBufferSize)
	jobs := r.jobChan
	for {
		select {
		case <-r.done:
			return
		case job := <-jobs:
			filename := job.filename
			status := job.status
			if status == JobStop {
				log.Info("job(uid: %s) file(%s) status(%d) is stop, job will be ignore", job.Uid(), filename, status)
				r.watcher.decideJob(job)
				continue
			}
			file := job.file
			if file == nil {
				log.Error("job(uid: %s) file(%s) released,job will be ignore", job.Uid(), filename)
				r.watcher.decideJob(job)
				continue
			}
			lastOffset, err := file.Seek(0, io.SeekCurrent)
			if err != nil {
				log.Error("can't get offset, file(name:%s) seek error, err: %v", filename, err)
				r.watcher.decideJob(job)
				continue
			}
			job.currentLines = 0

			startReadTime := time.Now()
			continueRead := 0
			isEOF := false
			wasSend := false
			readTotal := int64(0)
			processed := int64(0)
			backlogBuffer = backlogBuffer[:0]
			for {
				readBuffer = readBuffer[:readBufferSize]
				l, readErr := file.Read(readBuffer)
				if errors.Is(readErr, io.EOF) || l == 0 {
					isEOF = true
					job.eofCount++
					break
				}
				if readErr != nil {
					log.Error("file(name:%s) read error, err: %v", filename, err)
					break
				}
				read := int64(l)
				readBuffer = readBuffer[:read]
				now := time.Now()
				processed = 0
				for processed < read {
					index := int64(bytes.IndexByte(readBuffer[processed:], '\n'))
					if index == -1 {
						break
					}
					index += processed

					endOffset := lastOffset + readTotal + index
					if len(backlogBuffer) != 0 {
						backlogBuffer = append(backlogBuffer, readBuffer[processed:index]...)
						job.ProductEvent(endOffset, now, backlogBuffer)

						// Clean the backlog buffer after sending
						backlogBuffer = backlogBuffer[:0]
					} else {
						job.ProductEvent(endOffset, now, readBuffer[processed:index])
					}
					processed = index + 1
				}

				readTotal += read

				// The remaining bytes read are added to the backlog buffer
				if processed < read {
					backlogBuffer = append(backlogBuffer, readBuffer[processed:]...)

					// TODO check whether it is too long to avoid bursting the memory
					// if len(backlogBuffer)>max_bytes{
					//	log.Error
					//	break
					// }
				}

				wasSend = processed != 0
				if wasSend {
					continueRead++
					// According to the number of batches 2048, a maximum of one batch can be read,
					// and a single event is calculated according to 512 bytes, that is, the maximum reading is 1mb ,maxContinueRead = 16 by default
					// SSD recommends that maxContinueRead be increased by 3 ~ 5x
					if continueRead > maxContinueRead {
						break
					}
					if time.Since(startReadTime) > maxContinueReadTimeout {
						break
					}
				}
			}

			if wasSend {
				job.eofCount = 0
				job.lastActiveTime = time.Now()
			}

			l := len(backlogBuffer)
			if l > 0 {
				// When it is necessary to back off the offset, check whether it is inactive to collect the last line
				wasLastLineSend := false
				if isEOF && !wasSend {
					if time.Since(job.lastActiveTime) >= inactiveTimeout {
						// Send "last line"
						endOffset := lastOffset + readTotal
						job.ProductEvent(endOffset, time.Now(), backlogBuffer)
						job.lastActiveTime = time.Now()
						wasLastLineSend = true
						// Ignore the /n that may be written next.
						// Because the "last line" of the collection thinks that either it will not be written later,
						// or it will write /n first, and then write the content of the next line,
						// it is necessary to seek a position later to ignore the /n that may be written
						_, err = file.Seek(1, io.SeekCurrent)
						if err != nil {
							log.Error("can't set offset, file(name:%s) seek error: %v", filename, err)
						}
					} else {
						// Enable the job to escape and collect the last line
						job.eofCount = 0
					}
				}
				// Fallback accumulated buffer offset
				if !wasLastLineSend {
					backwardOffset := int64(-l)
					_, err = file.Seek(backwardOffset, io.SeekCurrent)
					if err != nil {
						log.Error("can't set offset, file(name:%s) seek error: %v", filename, err)
					}
				}
			}
			r.watcher.decideJob(job)
		}
	}
}
