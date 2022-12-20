/*
Copyright 2022 Loggie Authors

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

package flowdatapool

import (
	"sync"

	"github.com/loggie-io/loggie/pkg/core/api"
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
