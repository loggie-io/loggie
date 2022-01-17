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

package invoker

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
)

type DefaultInvocation struct {
	producer  api.Producer
	consumers []api.Consumer
	selector  api.Selector
	event     *event.DefaultEvent
}

func NewDefaultInvocation(producer api.Producer, consumers []api.Consumer, selector api.Selector) *DefaultInvocation {
	return &DefaultInvocation{
		producer:  producer,
		consumers: consumers,
		selector:  selector,
	}
}

func (di *DefaultInvocation) Producer() api.Producer {
	return di.producer
}

func (di *DefaultInvocation) Consumers() []api.Consumer {
	return di.consumers
}

func (di *DefaultInvocation) Selector() api.Selector {
	return di.selector
}

func (di *DefaultInvocation) Event() api.Event {
	return di.event
}

func (di *DefaultInvocation) AppendConsumer(consumer api.Consumer) {
	if di.consumers == nil {
		di.consumers = make([]api.Consumer, 0)
	}
	di.consumers = append(di.consumers, consumer)
}
