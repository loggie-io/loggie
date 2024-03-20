package ack

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/source/filestream/config"
	"github.com/loggie-io/loggie/pkg/source/filestream/monitor"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
	"sync"
	"time"
)

var (
	globalAckChainHandler *ChainHandler
	ackLock               sync.Mutex
	Start                 = TaskType("start")
	Stop                  = TaskType("stop")
	largeAckSize          = 10 * 4096
)

type TaskType string
type persistenceFunc func(progress storage.CollectProgress)

type ack struct {
	done  bool
	mark  Mark
	start time.Time
	next  *ack
	first bool
}

func NewAckWith(event Mark) *ack {
	ack := newAck()
	ack.mark = event
	ack.start = time.Now()
	return ack
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

func (a *ack) Key() string {
	return a.mark.GetAckId()
}

func ReleaseAck(a *ack) {
	if a == nil {
		return
	}
	a = &ack{}
	ackPool.Put(a)
}

type Task struct {
	Epoch           *pipeline.Epoch
	PipelineName    string
	SourceName      string
	key             string
	ackTaskType     TaskType
	StopCountDown   *sync.WaitGroup
	persistenceFunc persistenceFunc
	ackMonitor      *monitor.AckMonitor
}

func NewAckTask(epoch *pipeline.Epoch, pipelineName string, sourceName string, ackMonitor *monitor.AckMonitor) *Task {
	return &Task{
		Epoch:         epoch,
		PipelineName:  pipelineName,
		SourceName:    sourceName,
		key:           fmt.Sprintf("%s:%s", pipelineName, sourceName),
		StopCountDown: &sync.WaitGroup{},
		ackMonitor:    ackMonitor,
	}
}

func (at *Task) Key() string {
	return at.key
}

func (at *Task) isParentOf(chain *JobAckChain) bool {
	return at.PipelineName == chain.PipelineName && at.SourceName == chain.SourceName
}

func (at *Task) isContain(m Mark) bool {
	return at.SourceName == m.GetSourceName() && at.Epoch.Equal(m.GetEpoch())
}

func (at *Task) NewAckChain(jobWatchUid string) *JobAckChain {
	return newJobAckChain(at.Epoch, at.PipelineName, at.SourceName, jobWatchUid)
}

func newJobAckChain(epoch *pipeline.Epoch, pipelineName string, sourceName string, jobWatchUid string) *JobAckChain {
	return &JobAckChain{
		Epoch:        epoch,
		PipelineName: pipelineName,
		SourceKey:    fmt.Sprintf("%s:%s", pipelineName, sourceName),
		SourceName:   sourceName,
		JobWatchUid:  jobWatchUid,
		Start:        time.Now(),
		allAck:       make(map[string]*ack),
	}
}

type JobAckChain struct {
	Epoch        *pipeline.Epoch
	PipelineName string
	SourceName   string
	JobWatchUid  string
	lastFileName string
	SourceKey    string
	Start        time.Time
	tail         *ack
	allAck       map[string]*ack
}

func (ac *JobAckChain) GetSourceKey() string {
	return ac.SourceKey
}

func (ac *JobAckChain) Key() string {
	return ac.JobWatchUid
}

func (ac *JobAckChain) Ack(mark Mark) {
	if !ac.Epoch.Equal(mark.GetEpoch()) {
		log.Warn("ack mark(%+v) epoch not equal ack chain: ack mark.Epoch(%+v) vs chain.Epoch(%+v)", mark, mark.GetEpoch(), ac.Epoch)
		return
	}

	a := NewAckWith(mark)
	oa, ok := ac.allAck[a.Key()]
	if ok {
		oa.done = true
		if oa.first {
			ac.ackLoop(oa)
		}
	} else {
		log.Error("cannot find ack ack mark(%+v)", mark)
	}
}

func (ac *JobAckChain) ackLoop(a *ack) {
	next := a
	prev := a
	// The definition of this variable cannot be ignored because prev will be released first
	prevState := a.mark
	for next != nil {
		if !next.done {
			next.first = true
			break
		}
		// this job ack chain all ack mark acked, delete ack chain directly
		if ac.tail.Key() == next.Key() {
			ac.tail = nil
		}
		// delete node cache
		delete(ac.allAck, next.Key())
		// delete ack chain node
		prev = next
		prevState = next.mark
		next = next.next
		prev.next = nil

		// release ack
		ReleaseAck(prev)
	}

	if prevState.GetProgress() != (storage.CollectProgress)(nil) {
		// persistence ack
		prevState.GetProgress().Save()
	}

}

// Append 追加进入待确认缓冲区
func (ac *JobAckChain) Append(mark Mark) {
	// 判断是否是当前纪元的mark
	if !ac.Epoch.Equal(mark.GetEpoch()) {
		log.Warn("ack mark(%+v) epoch not equal ack chain: ack mark.Epoch(%+v) vs chain.Epoch(%+v)", mark, mark.GetEpoch(), ac.Epoch)
		return
	}

	a := NewAckWith(mark)
	existAck, ok := ac.allAck[a.Key()]
	if ok {
		log.Error("append ack(%+v) exist: %+v; last fileName: %s", mark, existAck.mark, ac.lastFileName)
		return
	}
	ac.allAck[a.Key()] = a
	if ac.tail == nil {
		a.first = true
		ac.tail = a
		return
	}
	ac.tail.next = a
	ac.tail = a

	// Check whether the chain is too long
	l := len(ac.allAck)
	if l > largeAckSize {
		log.Error("JobAckChain is too long(%d). stream(%s)", l, ac.tail.mark.GetName())
	}
}

type ChainHandler struct {
	done         chan struct{}
	ackConfig    config.AckConfig
	sinkCount    int
	countDown    *sync.WaitGroup
	ackTaskChan  chan *Task
	ackTasks     map[string]*Task
	jobAckChains map[string]*JobAckChain
	appendChan   chan []Mark
	AckChan      chan []Mark
	ackMonitors  map[string]*monitor.AckMonitor
}

func GetOrCreateShareAckChainHandler(sinkCount int, ackConfig config.AckConfig) *ChainHandler {
	if globalAckChainHandler != nil {
		return globalAckChainHandler
	}
	ackLock.Lock()
	defer ackLock.Unlock()
	if globalAckChainHandler != nil {
		return globalAckChainHandler
	}
	globalAckChainHandler = NewAckChainHandler(sinkCount, ackConfig)
	return globalAckChainHandler
}

func NewAckChainHandler(sinkCount int, ackConfig config.AckConfig) *ChainHandler {
	handler := &ChainHandler{
		done:         make(chan struct{}),
		ackConfig:    ackConfig,
		sinkCount:    sinkCount,
		jobAckChains: make(map[string]*JobAckChain),
		appendChan:   make(chan []Mark),
		AckChan:      make(chan []Mark, sinkCount),
		countDown:    &sync.WaitGroup{},
		ackTasks:     make(map[string]*Task),
		ackTaskChan:  make(chan *Task),
		ackMonitors:  make(map[string]*monitor.AckMonitor),
	}
	go handler.run()
	return handler
}

func (ach *ChainHandler) StopTask(task *Task) {
	task.ackTaskType = Stop
	task.StopCountDown.Add(1)
	ach.ackTaskChan <- task
	task.StopCountDown.Wait()
}

func (ach *ChainHandler) Stop() {
	close(ach.done)
	ach.countDown.Wait()
}

func (ach *ChainHandler) StartTask(task *Task) {
	task.ackTaskType = Start
	ach.ackTaskChan <- task
}

func (ach *ChainHandler) run() {
	ach.countDown.Add(1)
	log.Info("ack chain handler start")
	maintenanceTicker := time.NewTicker(ach.ackConfig.MaintenanceInterval)
	monitorExportTicker := time.NewTicker(ach.ackConfig.MonitorExportInterval)
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
			if taskType == Start {
				log.Info("AckStart:%s", ackTask.Key())
				_, ok := ach.ackTasks[ackTask.Key()]
				if ok {
					log.Error("ackTask exist: %s", ackTask.Key())
				} else {
					ach.ackTasks[ackTask.Key()] = ackTask
				}
				ach.ackMonitors[ackTask.Key()] = ackTask.ackMonitor
			} else if taskType == Stop {
				delete(ach.ackTasks, ackTask.Key())
				log.Info("AckStop:%s", ackTask.Key())
				log.Info("delete ackTasks:%s", ackTask.Key())
				// stop all ack jobs
				for _, chain := range ach.jobAckChains {
					if ackTask.isParentOf(chain) {
						delete(ach.jobAckChains, chain.Key())
						//chain.Release()
						log.Info("delete jobAckChains:%s", ackTask.Key())
					}
				}
				delete(ach.ackMonitors, ackTask.Key())
				ackTask.StopCountDown.Done()
			}
		case marks := <-ach.appendChan:
			if len(marks) == 0 {
				return
			}
			for _, mark := range marks {
				name := mark.GetName()
				if chain, ok := ach.jobAckChains[name]; ok {
					chain.Append(mark)
				} else {
					// new ack chain
					create := false
					for _, task := range ach.ackTasks {
						if task.isContain(mark) {
							create = true
							ackChain := task.NewAckChain(mark.GetName())
							ach.jobAckChains[mark.GetName()] = ackChain
							ackChain.Append(mark)
							break
						}
					}
					if !create {
						log.Debug("append ack mark of source has stopped: %+v", mark)
					}
				}
			}

		case marks := <-ach.AckChan:
			for _, mark := range marks {
				if chain, ok := ach.jobAckChains[mark.GetName()]; ok {
					chain.Ack(mark)
				} else {
					log.Debug("ack ack mark of source has stopped: %+v", mark)
				}
			}
		case <-maintenanceTicker.C:
			// Delete empty chain
			for _, chain := range ach.jobAckChains {
				delete(ach.jobAckChains, chain.Key())
			}
			// TODO Check whether a chain has not been acked completely for too long
		case <-monitorExportTicker.C:
			// Export Ack
			for name, chain := range ach.jobAckChains {
				ach.report(name, chain)
			}
		}
	}
}

func (ach *ChainHandler) report(name string, chain *JobAckChain) {
	ackMonitor, exist := ach.ackMonitors[chain.GetSourceKey()]
	if !exist {
		return
	}
	backLogLen := len(chain.allAck)
	ackMonitor.AckCollect(name, backLogLen)
}
