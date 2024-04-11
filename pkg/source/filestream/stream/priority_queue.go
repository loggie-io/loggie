package stream

import (
	"sync"
)

type PriorityQueueData struct {
	key   int64
	value interface{}
}

type PriorityQueue struct {
	caches   []PriorityQueueData
	size     int
	capacity int
	mutex    sync.RWMutex
	max      *PriorityQueueData
}

// NewStreamPriorityQueue 大根堆要优先队列
func NewStreamPriorityQueue(size int) *PriorityQueue {
	return &PriorityQueue{
		caches:   make([]PriorityQueueData, size),
		capacity: size,
	}
}

func (q *PriorityQueue) GetQueueTop() *PriorityQueueData {
	q.mutex.RLock()
	defer q.mutex.RUnlock()
	if q.size == 0 {
		return nil
	}
	return &q.caches[1]
}

func (q *PriorityQueue) swap(x *PriorityQueueData, y *PriorityQueueData) {
	tmp := *x
	*x = *y
	*y = tmp
}

func (q *PriorityQueue) adjustUp() {
	child := q.size
	parent := (child - 1) / 2
	for q.caches[parent].key < q.caches[child].key {
		q.swap(&(q.caches[parent]), &(q.caches[child])) //进行交换
		child = parent
		parent = (child - 1) / 2
	}
}

func (q *PriorityQueue) adjustDown(n int, parent int) {
	child := parent*2 + 1
	for child < n {
		//比较左孩子还是右孩子大
		if child+1 < n && q.caches[child+1].key > q.caches[child].key {
			child++
		}
		if q.caches[child].key > q.caches[parent].key {
			q.swap(&q.caches[child], &q.caches[parent])
			parent = child
			child = parent*2 + 1
		} else {
			break
		}
	}
}

// SureEnQueue 确保一定能添加进去，当队列长度不足的时候会去掉最小值，加入节点W
func (q *PriorityQueue) SureEnQueue(data *PriorityQueueData) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.size == q.capacity {
		q.DeQueue()
	}
	if q.max == nil {
		q.max = data
	} else {
		if data.key >= q.max.key {
			q.max = data
		}
	}
	q.enQueue(data)
}

func (q *PriorityQueue) enQueue(data *PriorityQueueData) {
	if q.size == q.capacity {
		return
	}

	q.size++

	var i int
	for i = q.size; q.caches[i/2].key > data.key; i /= 2 {
		q.caches[i] = q.caches[i/2]
	}
	q.caches[i] = *data
}

func (q *PriorityQueue) DeQueue() *PriorityQueueData {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.size == 0 {
		return nil
	}

	minElements := q.caches[1]
	lastElements := q.caches[(q.size)]

	var i int
	var child int

	for i = 1; i*2 <= q.size; i = child {
		child = i * 2

		//左右子节点比较,选较小的一方
		if child != q.size && (q.caches[child+1].key < q.caches[child].key) {
			child++
		}

		//最后一个元素和子节点比较，如果最后一个元素大，上提子节点
		if lastElements.key > q.caches[child].key {
			q.caches[i] = q.caches[child]
		} else {
			break
		}
	}
	q.size--
	q.caches[i] = lastElements
	return &minElements
}

// Front 查找最大节点
func (q *PriorityQueue) Front() *PriorityQueueData {
	q.mutex.RLock()
	defer q.mutex.RUnlock()
	if q.size == 0 {
		return nil
	}
	return q.max
}

func (q *PriorityQueue) Size() int {
	q.mutex.RLock()
	defer q.mutex.RUnlock()
	return q.size
}

func (q *PriorityQueue) Reset() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for i := 0; i < len(q.caches); i++ {
		q.caches[i].key = 0
		q.caches[i].value = nil
	}
	q.size = 0
}
