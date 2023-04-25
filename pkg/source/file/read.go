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
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
)

const (
	SystemStateKey = event.SystemKeyPrefix + "State"
)

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
		jobChan:   make(chan *Job, config.readChanSize),
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
		go r.cleanData()
	})
}

func (r *Reader) cleanData() {
	timeout := time.NewTimer(r.config.CleanDataTimeout)
	defer timeout.Stop()

	for {
		select {
		case <-timeout.C:
			return
		case j := <-r.jobChan:
			r.watcher.DecideJob(j)
		}
	}
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
	backlogBuffer := make([]byte, 0, readBufferSize)
	readBuffer := make([]byte, readBufferSize)
	jobs := r.jobChan
	processChain := r.buildProcessChain()
	for {
		select {
		case <-r.done:
			return
		case job := <-jobs:
			if ctx, err := NewJobCollectContextAndValidate(job, readBuffer, backlogBuffer); err == nil {
				processChain.Process(ctx)
			}
			r.watcher.DecideJob(job)
		}
	}
}

func (r *Reader) buildProcessChain() ProcessChain {
	return NewProcessChain(r.config)
}
