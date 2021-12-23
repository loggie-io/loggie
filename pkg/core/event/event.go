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
	"flag"
	"fmt"
	"loggie.io/loggie/pkg/core/api"
	"strings"
	"sync"
)

const (
	SystemKeyPrefix  = "system"
	PrivateKeyPrefix = "@private"

	SystemPipelineKey    = SystemKeyPrefix + "PipelineName"
	SystemSourceKey      = SystemKeyPrefix + "SourceName"
	SystemCollectTimeKey = SystemKeyPrefix + "CollectTime"

	Body = "body"
)

var (
	commonPoolCapacity int
)

func init() {
	flag.IntVar(&commonPoolCapacity, "event.pool.capacity", 8194, "Event pool capacity")
}

type DefaultEvent struct {
	H map[string]interface{} `json:"header"`
	B []byte                 `json:"body"`
}

func NewEvent(header map[string]interface{}, body []byte) *DefaultEvent {
	return &DefaultEvent{
		H: header,
		B: body,
	}
}

func newBlankEvent() *DefaultEvent {
	return &DefaultEvent{}
}

func (de *DefaultEvent) Source() string {
	return de.H[SystemSourceKey].(string)
}

func (de *DefaultEvent) Header() map[string]interface{} {
	return de.H
}

func (de *DefaultEvent) Body() []byte {
	return de.B
}

func (de *DefaultEvent) Fill(header map[string]interface{}, body []byte) {
	de.H = header
	de.B = body
}

func (de *DefaultEvent) Release() {
	// clean event
	de.H = nil
	de.B = nil
}

func (de *DefaultEvent) String() string {
	var sb strings.Builder
	sb.WriteString("header:")
	sb.WriteString("{")
	for k, v := range de.Header() {
		sb.WriteString(k)
		sb.WriteString(" : ")
		sb.WriteString(fmt.Sprintf("%+v", v))
		sb.WriteString(", ")
	}
	sb.WriteString("}; body:{")
	sb.WriteString(string(de.Body()))
	sb.WriteString("}")
	return sb.String()
}

func initCommonEventPool(capacity int) *Pool {
	return NewPool(capacity, func() api.Event {
		return newBlankEvent()
	})
}

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
