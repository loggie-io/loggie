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

package cost

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/sink"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"math/rand"
	"time"
)

const Type = "cost"

func init() {
	pipeline.Register(api.INTERCEPTOR, Type, makeInterceptor)
}

func makeInterceptor(info pipeline.Info) api.Component {
	return &Interceptor{
		config: &Config{},
	}
}

type Interceptor struct {
	done   chan struct{}
	name   string
	config *Config
	full   bool
	enable bool
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

	i.enable = true
	if i.config.Sampling == 10000 {
		i.full = true
	}
	if i.config.Sampling == 0 {
		i.enable = false
	}
	log.Info("%s sampling: %d", i.String(), i.config.Sampling)
}

func (i *Interceptor) Start() {
}

func (i *Interceptor) Stop() {

}

func (i *Interceptor) Intercept(invoker sink.Invoker, invocation sink.Invocation) api.Result {
	if !i.enable {
		return invoker.Invoke(invocation)
	}
	start := time.Now()
	result := invoker.Invoke(invocation)
	if i.full || (rand.Intn(10000) < i.config.Sampling) {
		log.Info("sink.invoker cost: %dms", time.Since(start)/time.Millisecond)
	}
	return result
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
