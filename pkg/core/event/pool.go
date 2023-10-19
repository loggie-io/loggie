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

package event

import (
	"container/list"
	"context"
	"sync"

	"github.com/loggie-io/loggie/pkg/core/api"
)

func NewDefaultPool(capacity int) *Pool {
	return NewPool(capacity, func() api.Event {
		return newBlankEvent()
	})
}

func NewPool(capacity int, factory Factory) *Pool {
	pool := &Pool{
		capacity:     capacity,
		free:         capacity,
		factory:      factory,
		events:       make([]api.Event, capacity),
		lock:         &sync.Mutex{},
		requestQueue: list.New(),
	}

	for i := 0; i < capacity; i++ {
		pool.events[i] = pool.factory()
	}
	return pool
}

type Pool struct {
	capacity     int
	free         int
	factory      Factory
	events       []api.Event
	lock         *sync.Mutex
	requestQueue *list.List
}

func (p *Pool) GetNContext(ctx context.Context, n int) ([]api.Event, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if n > p.capacity {
		panic("out of pool capacity")
	}
	p.lock.Lock()
	var notify request
	var el *list.Element
	for {
		if p.free >= n {
			if el != nil {
				p.requestQueue.Remove(el)
			}
			break
		}
		if notify.C == nil {
			notify.init(n)
			el = p.requestQueue.PushBack(&notify)
		}
		p.lock.Unlock()
		select {
		case <-ctx.Done():
			p.lock.Lock()
			p.requestQueue.Remove(el)
			p.lock.Unlock()
			return nil, false
		case <-notify.C:
		}
		p.lock.Lock()
	}
	p.free -= n
	events := make([]api.Event, n)
	copy(events, p.events[p.free:p.free+n])
	p.notifyAndUnlock()
	for i := range events {
		events[i].Release()
	}
	return events, true
}

func (p *Pool) GetN(n int) []api.Event {
	events, ok := p.GetNContext(context.Background(), n)
	if !ok {
		panic("unknown expected GetNContext fail")
	}
	return events
}

func (p *Pool) GetContext(ctx context.Context) (api.Event, bool) {
	events, ok := p.GetNContext(ctx, 1)
	if !ok {
		return nil, false
	}
	return events[0], true
}

func (p *Pool) Get() api.Event {
	event, ok := p.GetContext(context.Background())
	if !ok {
		panic("unknown expected Get fail")
	}
	return event
}

func (p *Pool) notifyAndUnlock() {
	defer p.lock.Unlock()
	if p.free == 0 {
		return
	}
	for el := p.requestQueue.Front(); el != nil; el = el.Next() {
		notify := el.Value.(*request)
		if p.free >= notify.N {
			select {
			case notify.C <- struct{}{}:
				break
			default:
			}
		}
	}

}

// Put release events to the pool.
func (p *Pool) Put(events ...api.Event) {
	p.lock.Lock()
	if p.free+len(events) > len(p.events) {
		panic("unknown expected event put")
	}
	copy(p.events[p.free:], events)
	p.free += len(events)
	p.notifyAndUnlock()
}

// PutAll release events to the pool.
// Deprecated: Use Put instead
func (p *Pool) PutAll(events []api.Event) {
	p.Put(events...)
}

type request struct {
	C chan struct{}
	N int
}

func (r *request) init(n int) {
	r.C = make(chan struct{})
	r.N = n
}
