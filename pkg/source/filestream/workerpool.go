package filestream

import (
	"errors"
	"github.com/loggie-io/loggie/pkg/core/log"
	"sync"
	"sync/atomic"
)

const (
	StateRunning   uint32 = 0
	StateStopped   uint32 = 1
	NumFileWorkers int    = 10
)

// WorkerPool is a blocked worker pool inspired by https://github.com/gammazero/workerpool/
type WorkerPool struct {
	inNums     uint64
	outNums    uint64
	curWorkers int

	maxWorkers int
	workChan   chan func()

	countDown *sync.WaitGroup
	taskDone  *sync.Cond
	state     uint32
	sync.Mutex
	once sync.Once
}

// New creates and starts a pool of worker goroutines.
func newWorkerPool(maxWorkers int, queueSize int) *WorkerPool {
	if maxWorkers <= 0 {
		log.Error("WorkerNum must be greater than zero")
	}
	if queueSize <= 0 {
		log.Error("queueSize must be greater than zero")
	}

	w := &WorkerPool{
		maxWorkers: maxWorkers,
		workChan:   make(chan func(), queueSize),
		countDown:  &sync.WaitGroup{},
	}

	w.taskDone = sync.NewCond(w)

	w.start()
	return w
}

var (
	GlobalWorkerPool      *WorkerPool
	globalWorkerPoolMutex sync.Mutex
	// ErrStopped when stopped
	ErrStopped = errors.New("WorkerPool already stopped")
)

func InitWorkerPool() *WorkerPool {
	if GlobalWorkerPool != nil {
		return GlobalWorkerPool
	}

	globalWorkerPoolMutex.Lock()
	defer globalWorkerPoolMutex.Unlock()

	if GlobalWorkerPool != nil {
		return GlobalWorkerPool
	}

	GlobalWorkerPool = newWorkerPool(NumFileWorkers, 9*NumFileWorkers)
	return GlobalWorkerPool
}

func (w *WorkerPool) wokerFunc() {
	w.countDown.Add(1)

	defer func() {
		w.countDown.Done()
		log.Info("WorkerPool:w.countDown.Done()")
	}()
	w.Lock()
	w.curWorkers++
	log.Info("worker count:%d", w.curWorkers)
	w.Unlock()
	log.Info("WorkerPool:w.countDown.Add(1)")
LOOP:
	for fn := range w.workChan {
		fn()
		var needQuit bool
		w.Lock()
		w.outNums++
		if w.inNums == w.outNums {
			w.taskDone.Signal()
		}
		if w.curWorkers > w.maxWorkers {
			w.curWorkers--
			needQuit = true
		}
		w.Unlock()
		if needQuit {
			log.Info("workFunc break")
			break LOOP
		}
	}
	log.Info("workFunc Stop")
}

func (w *WorkerPool) start() {
	for i := 0; i < w.maxWorkers; i++ {
		go w.wokerFunc()
	}
}

// Resize ensures worker number match the expected one.
func (w *WorkerPool) Resize(maxWorkers int) {
	w.Lock()
	defer w.Unlock()
	for i := 0; i < maxWorkers-w.maxWorkers; i++ {
		go w.wokerFunc()
	}
	w.maxWorkers = maxWorkers
	// if maxWorkers<w.maxWorkers, redundant workers quit by themselves
}

// Submit enqueues a function for a worker to execute.
// Submit will block regardless if there is no free workers.
func (w *WorkerPool) Submit(fn func()) (err error) {
	if atomic.LoadUint32(&w.state) == StateStopped {
		return ErrStopped
	}

	w.Lock()
	w.inNums++
	w.Unlock()

	w.workChan <- fn
	return nil
}

func (w *WorkerPool) Wait() {
	w.Lock()
	defer w.Unlock()
	log.Info("workerPool w.taskDone.Wait()")
	for w.inNums != w.outNums {
		w.taskDone.Wait()
	}
	log.Info("workerPool w.taskDone.Wait() End")
}

// StopWait stops the worker pool and waits for all queued tasks tasks to complete.
func (w *WorkerPool) StopWait() {
	atomic.StoreUint32(&w.state, StateStopped)

	w.Wait()
	w.once.Do(func() {
		close(w.workChan)
		log.Info("workerPool close w.workChan")
	})
	log.Info("workerPool w.countDown.Wait() Start")
	w.countDown.Wait()
	log.Info("workerPool w.countDown.Wait() End")
}

func (w *WorkerPool) Restart() {
	atomic.StoreUint32(&w.state, StateRunning)
}
