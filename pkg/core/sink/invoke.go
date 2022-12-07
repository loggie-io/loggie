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

package sink

import (
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
)

type Invocation struct {
	Batch    api.Batch
	Sink     api.Sink
	FlowPool api.FlowDataPool
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

type SubscribeInvoker struct {
}

func (si *SubscribeInvoker) Invoke(invocation Invocation) api.Result {
	pool := invocation.FlowPool
	if pool.IsEnabled() {
		t1 := time.Now()
		result := invocation.Sink.Consume(invocation.Batch)
		t2 := time.Now()
		microseconds := t2.Sub(t1).Microseconds()
		pool.EnqueueRTT(microseconds)
		if result.Status() != api.SUCCESS {
			pool.PutFailedResult(result)
		}
		return result
	}

	return invocation.Sink.Consume(invocation.Batch)
}
