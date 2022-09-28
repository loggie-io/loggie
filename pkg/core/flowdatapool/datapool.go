package flowdatapool

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"sync"
)

type DataPool struct {
	q            *sliceQueue
	failedResult chan api.Result
	enabled      bool
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
		q:            newSliceQueue(n),
		failedResult: make(chan api.Result),
	}
}

func (dataPool *DataPool) EnqueueRTT(f int64) {
	if dataPool.enabled {
		dataPool.q.enqueue(f)
	}
}

func (dataPool *DataPool) DequeueAllRtt() []int64 {
	return dataPool.q.dequeueAll()
}

func (dataPool *DataPool) PutFailedResult(result api.Result) {
	if dataPool.enabled {
		dataPool.failedResult <- result
	}
}

func (dataPool *DataPool) GetFailedChannel() chan api.Result {
	return dataPool.failedResult
}

func (dataPool *DataPool) IsEnabled() bool {
	return dataPool.enabled
}

func (dataPool *DataPool) SetEnabled(enabled bool) {
	dataPool.enabled = enabled
}
