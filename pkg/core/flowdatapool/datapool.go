package flowdatapool

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"sync"
)

type DataPool struct {
	q             *sliceQueue
	failedResult  chan api.Result
	sourceBlocked bool
}

type sliceQueue struct {
	data []int64
	mu   sync.Mutex
	size int
	len  int
}

func newSliceQueue(n int) (q *sliceQueue) {
	return &sliceQueue{
		data: make([]int64, 0, n),
		size: n,
	}
}

func (q *sliceQueue) enqueue(V int64) {
	q.mu.Lock()
	if q.len == q.size {
		q.data = q.data[1:]
		q.len--
	}
	q.data = append(q.data, V)
	q.len++
	q.mu.Unlock()
}

func (q *sliceQueue) dequeue() int64 {
	q.mu.Lock()
	if len(q.data) == 0 {
		q.mu.Unlock()
		return 0
	}
	v := q.data[0]
	q.data = q.data[1:]
	q.mu.Unlock()
	return v
}

func (q *sliceQueue) dequeueAll() []int64 {
	q.mu.Lock()
	if len(q.data) == 0 {
		q.mu.Unlock()
		return nil
	}
	v := q.data
	q.data = make([]int64, 0, q.size)
	q.len = 0
	q.mu.Unlock()
	return v
}

func InitDataPool(n int) *DataPool {
	return &DataPool{
		q:             newSliceQueue(n),
		failedResult:  make(chan api.Result),
		sourceBlocked: false,
	}
}

func (dataPool *DataPool) EnqueueRTT(f int64) {
	dataPool.q.enqueue(f)
}

func (dataPool *DataPool) DequeueAllRtt() []int64 {
	return dataPool.q.dequeueAll()
}

func (dataPool *DataPool) PutFailedResult(result api.Result) {
	dataPool.failedResult <- result
}

func (dataPool *DataPool) GetFailedChannel() chan api.Result {
	return dataPool.failedResult
}

func (dataPool *DataPool) SetSourceBlocked() {
	dataPool.sourceBlocked = true
}

func (dataPool *DataPool) UnSetSourceBlocked() {
	dataPool.sourceBlocked = false
}

func (dataPool *DataPool) GetSourceBlocked() bool {
	return dataPool.sourceBlocked
}
