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

package api

import "loggie.io/loggie/pkg/core/cfg"

const (
	FAIL    = Status(0)
	SUCCESS = Status(1)
	TIMEOUT = Status(2)
	DROP    = Status(3)

	SOURCE      = Category("source")
	QUEUE       = Category("queue")
	SINK        = Category("sink")
	INTERCEPTOR = Category("interceptor")
	SELECTOR    = Category("selector")

	VERSION = "0.0.1"
)

type Status int32
type Category string
type Type string

type Context interface {
	Name() string
	Category() Category
	Type() Type
	Properties() cfg.CommonCfg
}

type Lifecycle interface {
	Init(context Context)
	Start() // nonblock
	Stop()
}

type Describable interface {
	Category() Category
	Type() Type
	String() string
}

type Event interface {
	Header() map[string]interface{}
	Body() []byte
	Fill(header map[string]interface{}, body []byte)
	Release()
	String() string
	Source() string
}

type Batch interface {
	Meta() map[string]interface{}
	Events() []Event
	Release()
}

type Result interface {
	Status() Status
	ChangeStatusTo(status Status)
	Error() error
}

type Component interface {
	Lifecycle
	Describable
	Config
}

type Config interface {
	Config() interface{}
}

// thread safe
type ProductFunc func(event Event) Result

type Producer interface {
	ProductLoop(productFunc ProductFunc)
}

type Consumer interface {
	Consume(batch Batch) Result
}

type Source interface {
	Component
	Producer
	Commit(events []Event)
}

type Sink interface {
	Component
	Consumer
}

type OutFunc func(batch Batch) Result

type Queue interface {
	Component
	In(event Event)
	Out() Batch
	OutChan() chan Batch
	OutLoop(outFunc OutFunc)
}

type Invocation interface {
	Consumers() []Consumer
	Selector() Selector
	Event() Event
	Batch() Batch
	Queue() Queue
}

type Invoker interface {
	Invoke(invocation Invocation) Result
}

type Interceptor interface {
	Component
}

type Selector interface {
	Component
	Select(event Event, consumers []Consumer) []Consumer
}

type Manager interface {
	Component
}
