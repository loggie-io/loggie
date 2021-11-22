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

package metric

import (
	"fmt"
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/sink"
	"loggie.io/loggie/pkg/eventbus"
	"loggie.io/loggie/pkg/pipeline"
	"time"
)

const Type = "metric"

func init() {
	pipeline.Register(api.INTERCEPTOR, Type, makeInterceptor)
}

func makeInterceptor(info pipeline.Info) api.Component {
	return &Interceptor{
		done:         make(chan struct{}),
		pipelineName: info.PipelineName,
		config:       &Config{},
	}
}

type Interceptor struct {
	done         chan struct{}
	pipelineName string
	name         string
	config       *Config
	setting      map[string]interface{}
	Period       time.Duration // interval
	topic        string
}

func (i *Interceptor) Config() interface{} {
	return i.config
}

func (i *Interceptor) Category() api.Category {
	return api.INTERCEPTOR
}

func (i *Interceptor) Type() api.Type {
	return Type
}

func (i *Interceptor) String() string {
	return fmt.Sprintf("%s/%s", i.Category(), i.Type())
}

func (i *Interceptor) Init(context api.Context) {
	i.name = context.Name()
	i.Period = i.config.Period
}

func (i *Interceptor) Start() {
}

func (i *Interceptor) Stop() {
	close(i.done)
}

func (i *Interceptor) Intercept(invoker sink.Invoker, invocation sink.Invocation) api.Result {
	result := invoker.Invoke(invocation)
	i.reportMetric(invocation.Batch, result)
	return result
}

func (i *Interceptor) reportMetric(batch api.Batch, result api.Result) {
	es := batch.Events()
	l := len(es)
	if l == 0 {
		return
	}
	isSuccess := result.Status() == api.SUCCESS
	source2es := make(map[string]int)
	for _, e := range es {
		sourceName := e.Source()
		c := source2es[sourceName]
		c++
		source2es[sourceName] = c
	}
	for k, v := range source2es {
		sinkMetricData := eventbus.SinkMetricData{
			BaseMetric: eventbus.BaseMetric{
				PipelineName: i.pipelineName,
				SourceName:   k,
			},
		}
		if isSuccess {
			sinkMetricData.SuccessEventCount = v
		} else {
			sinkMetricData.FailEventCount = v
		}
		eventbus.PublishOrDrop(eventbus.SinkMetricTopic, sinkMetricData)
	}
}

func (i *Interceptor) Order() int {
	return i.config.Order
}

func (i *Interceptor) BelongTo() (componentTypes []string) {
	return i.config.BelongTo
}

func (i *Interceptor) IgnoreRetry() bool {
	return false
}
