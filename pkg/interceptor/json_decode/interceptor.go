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

package json_decode

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"strings"
)

const Type = "jsonDecode"

func init() {
	pipeline.Register(api.INTERCEPTOR, Type, makeInterceptor)
}

var (
	json = jsoniter.ConfigFastest
)

func makeInterceptor(info pipeline.Info) api.Component {
	return &Interceptor{
		config: &Config{},
	}
}

type Interceptor struct {
	name   string
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

func (i *Interceptor) Init(context api.Context) {
	i.name = context.Name()
}

func (i *Interceptor) Start() {
}

func (i *Interceptor) Stop() {

}

func (i *Interceptor) Intercept(invoker source.Invoker, invocation source.Invocation) api.Result {
	e := invocation.Event
	if err := i.process(e); err != nil {
		log.Error("json decode event %s error: %v", e.String(), err)
	}
	return invoker.Invoke(invocation)
}

func (i *Interceptor) process(e api.Event) error {
	body := e.Body()
	header := e.Header()
	if header == nil {
		// TODO get from pool
		header = make(map[string]interface{})
	}

	message := make(map[string]interface{})
	err := json.Unmarshal(body, &message)
	if err != nil {
		return errors.WithMessagef(err, "json unmarshal message: %s error: %v", body, err)
	}

	// TODO
	for k, v := range message {
		// should not override system reserved key in header
		if strings.HasPrefix(k, event.SystemKeyPrefix) {
			if _, ok := header[k]; ok {
				continue
			}
		}
		if strings.HasPrefix(k, event.PrivateKeyPrefix) {
			continue
		}
		header[k] = v
	}

	for _, f := range i.config.DropFields {
		if f == "body" {
			body = nil
		} else {
			delete(header, f)
		}
	}

	if i.config.BodyKey == "" {
		e.Fill(e.Meta(), header, body)
		return nil
	}

	val, ok := header[i.config.BodyKey].(string)
	if !ok {
		return errors.New("message value is not string")
	}
	e.Fill(e.Meta(), header, []byte(val))
	return nil
}
