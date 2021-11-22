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

package limit

import (
	"fmt"
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/core/source"
	"loggie.io/loggie/pkg/pipeline"
)

const Type = "rateLimit"

func init() {
	pipeline.Register(api.INTERCEPTOR, Type, makeInterceptor)
}

func makeInterceptor(info pipeline.Info) api.Component {
	return &Interceptor{
		config: &Config{},
	}
}

type Interceptor struct {
	name   string
	config *Config
	qps    int
	l      Limiter
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
	i.qps = i.config.Qps
}

func (i *Interceptor) Start() {
	log.Info("rate limit: qps->%d", i.qps)
	i.l = newUnsafeBased(i.qps, WithoutLock())
}

func (i *Interceptor) Stop() {

}

func (i *Interceptor) Intercept(invoker source.Invoker, invocation source.Invocation) api.Result {
	i.l.Take()
	return invoker.Invoke(invocation)
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
