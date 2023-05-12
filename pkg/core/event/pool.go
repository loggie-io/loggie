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
	"github.com/loggie-io/loggie/pkg/core/api"
	"sync"
)

type Factory func() api.Event

type Pool struct {
	capacity int
	free     int
	factory  Factory
	events   []api.Event
	lock     *sync.Mutex
	cond     *sync.Cond
}

func NewDefaultPool(capacity int) *Pool {
	return NewPool(capacity, func() api.Event {
		return newBlankEvent()
	})
}

func NewPool(capacity int, factory Factory) *Pool {
	pool := &Pool{
		capacity: capacity,
		free:     capacity,
		factory:  factory,
		events:   make([]api.Event, capacity),
		lock:     &sync.Mutex{},
	}
	pool.cond = sync.NewCond(pool.lock)

	for i := 0; i < capacity; i++ {
		pool.events[i] = pool.factory()
	}
	return pool
}

func (p *Pool) Get() api.Event {
	p.lock.Lock()

	for p.free == 0 {
		p.cond.Wait()
	}
	p.free--
	e := p.events[p.free]

	p.lock.Unlock()

	e.Release()
	return e
}

func (p *Pool) GetN(n int) []api.Event {
	es := make([]api.Event, n)
	p.lock.Lock()

	for i := 0; i < n; i++ {
		for p.free == 0 {
			p.cond.Wait()
		}
		p.free--
		e := p.events[p.free]
		es[i] = e
	}

	p.lock.Unlock()
	return es
}

func (p *Pool) Put(event api.Event) {
	p.lock.Lock()

	p.events[p.free] = event
	p.free++
	p.cond.Signal()

	p.lock.Unlock()
}

func (p *Pool) PutAll(events []api.Event) {
	p.lock.Lock()

	for _, e := range events {
		p.events[p.free] = e
		p.free++
	}
	p.cond.Broadcast()

	p.lock.Unlock()
}
