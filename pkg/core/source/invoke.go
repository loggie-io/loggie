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

package source

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/result"
)

type Invocation struct {
	Event api.Event
	Queue api.Queue
}

type Invoker interface {
	Invoke(invocation Invocation) api.Result
}

type AbstractInvoker struct {
	DoInvoke func(invocation Invocation) api.Result
}

func (ai *AbstractInvoker) Invoke(invocation Invocation) api.Result {
	return ai.DoInvoke(invocation)
}

// NewFakeInvoker usually used by unit test or benchmark
func NewFakeInvoker() *AbstractInvoker {
	return &AbstractInvoker{
		DoInvoke: func(invocation Invocation) api.Result {
			return result.Success()
		},
	}
}

// publish event to queue
type PublishInvoker struct {
}

func (i *PublishInvoker) Invoke(invocation Invocation) api.Result {
	invocation.Queue.In(invocation.Event)
	return result.Success()
}
