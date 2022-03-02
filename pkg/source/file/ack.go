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
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"sync"
	"time"
)

const (
	largeAckSize = 10 * 4096

	AckStart = AckTaskType("start")
	AckStop  = AckTaskType("stop")
)

type AckTaskType string

type AckTask struct {
	Epoch           *pipeline.Epoch
	PipelineName    string
	SourceName      string
	key             string
	ackTaskType     AckTaskType
	StopCountDown   *sync.WaitGroup
	persistenceFunc persistenceFunc
}

func NewAckTask(epoch *pipeline.Epoch, pipelineName string, sourceName string, persistenceFunc persistenceFunc) *AckTask {
	return &AckTask{
		Epoch:           epoch,
		PipelineName:    pipelineName,
		SourceName:      sourceName,
		key:             fmt.Sprintf("%s:%s", pipelineName, sourceName),
		StopCountDown:   &sync.WaitGroup{},
		persistenceFunc: persistenceFunc,
	}
}

func (at *AckTask) Key() string {
	return at.key
}

func (at *AckTask) isParentOf(chain *JobAckChain) bool {
	return at.PipelineName == chain.PipelineName && at.SourceName == chain.SourceName
}

func (at *AckTask) isContain(s *State) bool {
	return at.SourceName == s.SourceName && at.Epoch.Equal(s.Epoch)
}

func (at *AckTask) NewAckChain(jobWatchUid string) *JobAckChain {
	return newJobAckChain(at.Epoch, at.PipelineName, at.SourceName, jobWatchUid, at.persistenceFunc)
}

type persistenceFunc func(state *State)

type ack struct {
	next  *ack
	first bool
	done  bool
	state *State
	start time.Time
}

func NewAckWith(state *State) *ack {
	ack := newAck()
	ack.state = state
	ack.start = time.Now()
	return ack
}

func (a *ack) ChainKey() string {
	return a.state.WatchUid()
}

func (a *ack) Key() string {
	return a.state.EventUid
}

func getState(e api.Event) *State {
	if e == nil {
		panic("event is nil")
	}
	state, _ := e.Meta().Get(SystemStateKey)
	return state.(*State)
}

var ackPool = sync.Pool{
	New: func() interface{} {
		return &ack{}
	},
}

func newAck() *ack {
	ack := ackPool.Get().(*ack)
	return ack
}

func ReleaseAck(a *ack) {
	if a == nil {
		return
	}
	a = &ack{}
	ackPool.Put(a)
}

type JobAckChain struct {
	Epoch           *pipeline.Epoch
	PipelineName    string
	SourceName      string
	JobWatchUid     string
	lastFileName    string
	persistenceFunc persistenceFunc
	Start           time.Time
	tail            *ack
	allAck          map[string]*ack
}

func newJobAckChain(epoch *pipeline.Epoch, pipelineName string, sourceName string, jobWatchUid string, persistenceFunc persistenceFunc) *JobAckChain {
	return &JobAckChain{
		Epoch:           epoch,
		PipelineName:    pipelineName,
		SourceName:      sourceName,
		JobWatchUid:     jobWatchUid,
		persistenceFunc: persistenceFunc,
		Start:           time.Now(),
		allAck:          make(map[string]*ack),
	}
}

func (ac *JobAckChain) Release() {
	ac.tail = nil
	for _, a := range ac.allAck {
		ReleaseAck(a)
	}
	ac.allAck = nil
}

func (ac *JobAckChain) Key() string {
	return ac.JobWatchUid
}

func (ac *JobAckChain) Append(s *State) {
	if !ac.Epoch.Equal(s.Epoch) {
		log.Warn("state(%+v) epoch not equal ack chain: state.Epoch(%+v) vs chain.Epoch(%+v)", s, s.Epoch, ac.Epoch)
		return
	}
	a := NewAckWith(s)
	existAck, ok := ac.allAck[a.Key()]
	if ok {
		log.Error("append state(%+v) exist: %+v; last fileName: %s", s, existAck.state, ac.lastFileName)
		return
	}
	ac.allAck[a.Key()] = a
	if ac.tail == nil {
		a.first = true
		ac.tail = a
		ac.lastFileName = s.Filename
		return
	}
	ac.tail.next = a
	ac.tail = a

	// Check whether the chain is too long
	l := len(ac.allAck)
	if l > largeAckSize {
		log.Error("JobAckChain is too long(%d). head(%s) fileName: %s", l, ac.tail.state.WatchUid(), ac.tail.state.Filename)
	}
}

func (ac *JobAckChain) Ack(s *State) {
	if !ac.Epoch.Equal(s.Epoch) {
		log.Warn("state(%+v) epoch not equal ack chain: state.Epoch(%+v) vs chain.Epoch(%+v)", s, s.Epoch, ac.Epoch)
		return
	}
	a := NewAckWith(s)
	oa, ok := ac.allAck[a.Key()]
	if ok {
		oa.done = true
		if oa.first {
			ac.ackLoop(oa)
		}
	} else {
		log.Error("cannot find ack state(%+v)", s)
	}
}

func (ac *JobAckChain) ackLoop(a *ack) {
	next := a
	prev := a
	// The definition of this variable cannot be ignored because prev will be released first
	prevState := a.state
	for next != nil {
		if !next.done {
			next.first = true
			break
		}
		// this job ack chain all state acked, delete ack chain directly
		if ac.tail.Key() == next.Key() {
			ac.tail = nil
		}
		// delete node cache
		delete(ac.allAck, next.Key())
		// delete ack chain node
		prev = next
		prevState = next.state
		next = next.next
		prev.next = nil

		// release ack
		ReleaseAck(prev)
	}
	// persistence ack
	ac.persistenceFunc(prevState)
}

func (ac *JobAckChain) isEmpty() bool {
	return ac.tail == nil
}

type AckChainHandler struct {
	done         chan struct{}
	ackConfig    AckConfig
	sinkCount    int
	jobAckChains map[string]*JobAckChain
	appendChan   chan []*State
	ackChan      chan []*State
	countDown    *sync.WaitGroup
	ackTasks     map[string]*AckTask
	ackTaskChan  chan *AckTask
}

func NewAckChainHandler(sinkCount int, ackConfig AckConfig) *AckChainHandler {
	handler := &AckChainHandler{
		done:         make(chan struct{}),
		ackConfig:    ackConfig,
		sinkCount:    sinkCount,
		jobAckChains: make(map[string]*JobAckChain),
		appendChan:   make(chan []*State),
		ackChan:      make(chan []*State, sinkCount),
		countDown:    &sync.WaitGroup{},
		ackTasks:     make(map[string]*AckTask),
		ackTaskChan:  make(chan *AckTask),
	}
	go handler.run()
	return handler
}

func (ach *AckChainHandler) Stop() {
	close(ach.done)
	ach.countDown.Wait()
}

func (ach *AckChainHandler) StartTask(task *AckTask) {
	task.ackTaskType = AckStart
	ach.ackTaskChan <- task
}

func (ach *AckChainHandler) StopTask(task *AckTask) {
	task.ackTaskType = AckStop
	task.StopCountDown.Add(1)
	ach.ackTaskChan <- task
	task.StopCountDown.Wait()
}

func (ach *AckChainHandler) run() {
	ach.countDown.Add(1)
	log.Info("ack chain handler start")
	maintenanceTicker := time.NewTicker(ach.ackConfig.MaintenanceInterval)
	defer func() {
		maintenanceTicker.Stop()
		ach.countDown.Done()
		log.Info("ack chain handler stop")
	}()
	for {
		select {
		case <-ach.done:
			return
		case ackTask := <-ach.ackTaskChan:
			taskType := ackTask.ackTaskType
			if taskType == AckStart {
				_, ok := ach.ackTasks[ackTask.Key()]
				if ok {
					log.Error("ackTask exist: %s", ackTask.Key())
				} else {
					ach.ackTasks[ackTask.Key()] = ackTask
				}
			} else if taskType == AckStop {
				delete(ach.ackTasks, ackTask.Key())
				// stop all ack jobs
				for _, chain := range ach.jobAckChains {
					if ackTask.isParentOf(chain) {
						delete(ach.jobAckChains, chain.Key())
						chain.Release()
					}
				}
				ackTask.StopCountDown.Done()
			}
		case ss := <-ach.appendChan:
			for _, s := range ss {
				jobWatchUid := s.WatchUid()
				if chain, ok := ach.jobAckChains[jobWatchUid]; ok {
					chain.Append(s)
				} else {
					// new ack chain
					create := false
					for _, task := range ach.ackTasks {
						if task.isContain(s) {
							create = true
							ackChain := task.NewAckChain(jobWatchUid)
							ach.jobAckChains[ackChain.Key()] = ackChain
							ackChain.Append(s)
							break
						}
					}
					if !create {
						log.Debug("append state of source has stopped: %+v", s)
					}
				}
			}
		case ss := <-ach.ackChan:
			for _, s := range ss {
				jobWatchUid := s.WatchUid()
				if chain, ok := ach.jobAckChains[jobWatchUid]; ok {
					chain.Ack(s)
				} else {
					log.Debug("ack state of source has stopped: %+v", s)
				}
			}
		case <-maintenanceTicker.C:
			// Delete empty chain
			for _, chain := range ach.jobAckChains {
				if chain.isEmpty() {
					delete(ach.jobAckChains, chain.Key())
					chain.Release()
				}
			}
			// TODO Check whether a chain has not been acked completely for too long
		}
	}
}
