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

package maxbytes

import (
	"fmt"
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/source"
	"loggie.io/loggie/pkg/pipeline"
)

const Type = "maxbytes"

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
}

func (i *Interceptor) Start() {
}

func (i *Interceptor) Stop() {
	close(i.done)
}

func (i *Interceptor) Intercept(invoker source.Invoker, invocation source.Invocation) api.Result {
	event := invocation.Event
	body := event.Body()
	if len(body) > i.config.MaxBytes {
		event.Fill(event.Meta(), event.Header(), body[:i.config.MaxBytes])
	}
	return invoker.Invoke(invocation)
}

func (i *Interceptor) Order() int {
	return i.config.Order
}

func (i *Interceptor) BelongTo() (componentTypes []string) {
	return i.config.BelongTo
}

func (i *Interceptor) IgnoreRetry() bool {
	return true
}
