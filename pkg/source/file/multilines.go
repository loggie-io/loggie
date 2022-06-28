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
	"strings"
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/util"
)

const (
	MultiStart = MultiTaskType("start")
	MultiStop  = MultiTaskType("stop")
)

type MultiTaskType string

type MultiTask struct {
	mTaskType   MultiTaskType
	epoch       *pipeline.Epoch
	sourceName  string
	key         string
	config      MultiConfig
	matcher     util.Matcher
	eventPool   *event.Pool
	productFunc api.ProductFunc
	countDown   *sync.WaitGroup
}

func (mt *MultiTask) String() string {
	var s strings.Builder
	s.WriteString(mt.epoch.String())
	s.WriteString(":")
	s.WriteString(mt.sourceName)
	return s.String()
}

func (mt *MultiTask) isParentOf(mh *MultiHolder) bool {
	return mt.sourceName == mh.mTask.sourceName && mt.epoch.PipelineName == mh.mTask.epoch.PipelineName
}

func NewMultiTask(epoch *pipeline.Epoch, sourceName string, config MultiConfig, eventPool *event.Pool, productFunc api.ProductFunc) *MultiTask {
	return &MultiTask{
		epoch:       epoch,
		sourceName:  sourceName,
		key:         fmt.Sprintf("%s:%s", epoch.PipelineName, sourceName),
		config:      config,
		matcher:     util.MustCompile(config.Pattern),
		eventPool:   eventPool,
		productFunc: productFunc,
		countDown:   &sync.WaitGroup{},
	}
}

func (mt *MultiTask) newMultiHolder(state State) *MultiHolder {
	lineEnd := globalLineEnd.GetEncodeLineEnd(state.PipelineName, state.SourceName)
	return &MultiHolder{
		mTask:    mt,
		state:    state,
		initTime: time.Now(),

		lineEnd:       lineEnd,
		lineEndLength: int64(len(lineEnd)),
	}
}

func (mt *MultiTask) isContain(s *State) bool {
	return mt.sourceName == s.SourceName && mt.epoch.Equal(s.Epoch)
}

type MultiHolder struct {
	mTask *MultiTask
	state State

	content      []byte
	currentLines int
	currentSize  int64
	initTime     time.Time

	lineEnd       []byte
	lineEndLength int64
}

func (mh *MultiHolder) key() string {
	return mh.state.WatchUid()
}

func (mh *MultiHolder) append(event api.Event) {
	body := event.Body()
	sizeAvailable := mh.mTask.config.MaxBytes - int64(len(body)) - mh.currentSize
	if sizeAvailable <= 0 || mh.mTask.matcher.Match(body) {
		mh.flush()
	}
	state := *getState(event)
	mh.appendContent(body, state)
	mh.mTask.eventPool.Put(event)
}

func (mh *MultiHolder) appendContent(content []byte, state State) {
	if mh.currentSize == 0 {
		mh.initTime = time.Now()
		mh.state = state
	} else {
		// has content, add '\n'
		split := mh.lineEnd
		mh.content = append(mh.content, split...)
		mh.currentSize += int64(len(split))
		// change state
		mh.state.Filename = state.Filename
		mh.state.NextOffset = state.NextOffset
		mh.state.CollectTime = state.CollectTime
		mh.state.EventUid = state.EventUid
	}
	mh.currentLines++
	mh.currentSize += int64(len(content))
	mh.content = append(mh.content, content...)

	if mh.currentLines >= mh.mTask.config.MaxLines || mh.currentSize >= mh.mTask.config.MaxBytes {
		// flush immediately when (line maximum) or (the first line size exceed)
		log.Error("task(%s) multiline log exceeds limit: currentLines(%d),maxLines(%d);currentBytes(%d),maxBytes(%d)",
			mh.mTask.String(), mh.currentLines, mh.mTask.config.MaxLines, mh.currentSize, mh.mTask.config.MaxBytes)
		mh.flush()
	}
}

func (mh *MultiHolder) flush() {
	if mh.currentSize <= 0 {
		return
	}
	mh.state.ContentBytes = mh.currentSize + mh.lineEndLength
	state := &State{
		Epoch:        mh.state.Epoch,
		PipelineName: mh.state.PipelineName,
		SourceName:   mh.state.SourceName,
		Offset:       mh.state.Offset,
		NextOffset:   mh.state.NextOffset,
		Filename:     mh.state.Filename,
		CollectTime:  mh.state.CollectTime,
		ContentBytes: mh.state.ContentBytes,
		JobUid:       mh.state.JobUid,
		JobIndex:     mh.state.JobIndex,
		EventUid:     mh.state.EventUid,
		LineNumber:   mh.state.LineNumber,
		watchUid:     mh.state.watchUid,
	}
	// contentBuffer := make([]byte, mh.currentSize)
	// copy(contentBuffer, mh.content)
	contentBuffer := mh.content

	e := mh.mTask.eventPool.Get()
	e.Meta().Set(SystemStateKey, state)
	e.Fill(e.Meta(), e.Header(), contentBuffer)
	mh.mTask.productFunc(e)

	mh.content = make([]byte, 0)
	mh.currentLines = 0
	mh.currentSize = 0
}

type MultiProcessor struct {
	done          chan struct{}
	stopCountDown *sync.WaitGroup
	eventChan     chan api.Event
	taskChan      chan *MultiTask
	tasks         map[string]*MultiTask
	holderMap     map[string]*MultiHolder
}

func NewMultiProcessor() *MultiProcessor {
	mp := &MultiProcessor{
		done:          make(chan struct{}),
		stopCountDown: &sync.WaitGroup{},
		eventChan:     make(chan api.Event, 16),
		taskChan:      make(chan *MultiTask),
		tasks:         make(map[string]*MultiTask),
		holderMap:     make(map[string]*MultiHolder),
	}
	go mp.run()
	return mp
}

func (mp *MultiProcessor) StartTask(task *MultiTask) {
	task.mTaskType = MultiStart
	mp.taskChan <- task
}

func (mp *MultiProcessor) StopTask(task *MultiTask) {
	task.mTaskType = MultiStop
	task.countDown.Add(1)
	mp.taskChan <- task
	task.countDown.Wait()
}

func (mp *MultiProcessor) Process(event api.Event) api.Result {
	mp.eventChan <- event
	return result.Success()
}

func (mp *MultiProcessor) run() {
	mp.stopCountDown.Add(1)
	log.Info("multiline processor start")
	ticker := time.NewTicker(1 * time.Second)
	maintenanceTicker := time.NewTicker(20 * time.Hour)
	defer func() {
		ticker.Stop()
		maintenanceTicker.Stop()
		mp.stopCountDown.Done()
		log.Info("multiline processor stop")
	}()
	for {
		select {
		case <-mp.done:
			return
		case task := <-mp.taskChan:
			if task.mTaskType == MultiStart {
				_, ok := mp.tasks[task.key]
				if ok {
					log.Error("multiline task exist: %s", task.key)
				} else {
					mp.tasks[task.key] = task
				}
			} else if task.mTaskType == MultiStop {
				delete(mp.tasks, task.key)
				// stop all task holder
				for _, holder := range mp.holderMap {
					if task.isParentOf(holder) {
						delete(mp.holderMap, holder.key())
					}
				}
				task.countDown.Done()
			}
		case e := <-mp.eventChan:
			state := getState(e)
			watchUid := state.WatchUid()
			if mh, ok := mp.holderMap[watchUid]; ok {
				mh.append(e)
			} else {
				create := false
				for _, task := range mp.tasks {
					if task.isContain(state) {
						create = true
						mh := task.newMultiHolder(*state)
						mp.holderMap[mh.key()] = mh
						mh.append(e)
						break
					}
				}
				if !create {
					log.Debug("append event state of source has stopped: %+v", state)
				}
			}
		case <-ticker.C:
			mp.iterateFlush()
		case <-maintenanceTicker.C:
			mp.cleanUp()
		}
	}
}

func (mp *MultiProcessor) iterateFlush() {
	for _, holder := range mp.holderMap {
		if time.Since(holder.initTime) >= holder.mTask.config.Timeout {
			holder.flush()
		}
	}
}

func (mp *MultiProcessor) cleanUp() {
	for key, holder := range mp.holderMap {
		if holder.currentSize <= 0 {
			delete(mp.holderMap, key)
		}
	}
}
