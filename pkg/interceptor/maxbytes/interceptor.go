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
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"unicode/utf8"
)

const Type = "maxbytes"

func init() {
	pipeline.Register(api.INTERCEPTOR, Type, makeInterceptor)
}

func makeInterceptor(info pipeline.Info) api.Component {
	return NewInterceptor()
}

func NewInterceptor() *Interceptor {
	return &Interceptor{
		config: &Config{},
	}
}

type Interceptor struct {
	config *Config
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

func (i *Interceptor) Init(context api.Context) error {
	return nil
}

func (i *Interceptor) Start() error {
	return nil
}

func (i *Interceptor) Stop() {
}

func (i *Interceptor) Intercept(invoker source.Invoker, invocation source.Invocation) api.Result {
	event := invocation.Event
	body := event.Body()
	if len(body) > i.config.MaxBytes {
		event.Fill(event.Meta(), event.Header(), subUtf8(body, i.config.MaxBytes))
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

// subUtf8 Provides byte arrays that are truncated by byte length, or later if the last byte is not utf8 byte ending
func subUtf8(bytes []byte, maxBytes int) []byte {
	for i := maxBytes; i < len(bytes); i++ {
		if utf8.RuneStart(bytes[i]) {
			bytes = bytes[:i]
			break
		}
	}
	return bytes
}
