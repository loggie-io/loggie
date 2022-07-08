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
	"fmt"
	"strings"
	"sync"

	"github.com/loggie-io/loggie/pkg/core/api"
)

const (
	SystemKeyPrefix  = "system"
	PrivateKeyPrefix = "@private"

	SystemPipelineKey    = SystemKeyPrefix + "PipelineName"
	SystemSourceKey      = SystemKeyPrefix + "SourceName"
	SystemProductTimeKey = SystemKeyPrefix + "ProductTime"

	Body = "body"
)

type DefaultMeta struct {
	Properties map[string]interface{} `json:"properties"`
}

func NewDefaultMeta() *DefaultMeta {
	return &DefaultMeta{
		Properties: make(map[string]interface{}),
	}
}

func (dm *DefaultMeta) Source() string {
	return dm.Properties[SystemSourceKey].(string)
}

func (dm *DefaultMeta) Get(key string) (value interface{}, exist bool) {
	value, exist = dm.Properties[key]
	return value, exist
}

func (dm *DefaultMeta) GetAll() map[string]interface{} {
	return dm.Properties
}

func (dm *DefaultMeta) Set(key string, value interface{}) {
	dm.Properties[key] = value
}

func (dm *DefaultMeta) String() string {
	var s strings.Builder
	s.WriteString("{")
	for k, v := range dm.Properties {
		s.WriteString(k)
		s.WriteString(":")
		s.WriteString(fmt.Sprintf("%#v", v))
		s.WriteString(",")
	}
	s.WriteString("}")
	return s.String()
}

type DefaultEvent struct {
	H map[string]interface{} `json:"header"`
	B []byte                 `json:"body"`
	M api.Meta               `json:"meta"`
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

func (de *DefaultEvent) Meta() api.Meta {
	return de.M
}

func (de *DefaultEvent) Header() map[string]interface{} {
	if de.H == nil {
		de.H = make(map[string]interface{})
	}

	return de.H
}

func (de *DefaultEvent) Body() []byte {
	return de.B
}

func (de *DefaultEvent) Fill(meta api.Meta, header map[string]interface{}, body []byte) {
	de.H = header
	de.B = body
	de.M = meta
}

func (de *DefaultEvent) Release() {
	// clean event
	de.H = make(map[string]interface{})
	de.B = nil
	de.M = NewDefaultMeta()
}

func (de *DefaultEvent) String() string {
	var sb strings.Builder
	sb.WriteString("meta:")
	if de.M != nil {
		sb.WriteString(de.M.String())
	}
	sb.WriteString(";")
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
