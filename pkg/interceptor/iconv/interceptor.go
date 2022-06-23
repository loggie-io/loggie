/*
Copyright 2022 Loggie Authors

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
	"errors"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/util"
)

const Type = "iconv"

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
	i.name = context.Name()
	return nil
}

func (i *Interceptor) Start() error {
	return nil
}

func (i *Interceptor) Stop() {

}

func (i *Interceptor) Intercept(invoker source.Invoker, invocation source.Invocation) api.Result {
	e := invocation.Event
	if err := i.process(e); err != nil {
		log.Error("encode event %s error: %v", e.String(), err)
	}
	return invoker.Invoke(invocation)
}

func (i *Interceptor) process(e api.Event) error {
	if i.config.Charset == "utf-8" {
		return nil
	}

	codec, ok := util.AllEncodings[i.config.Charset]

	if !ok {
		log.Warn("unknown Charset('%v')", i.config.Charset)
		return errors.New(fmt.Sprintf("unknown Charset('%v')", i.config.Charset))
	}

	bytes, err := codec.NewDecoder().Bytes(e.Body())

	if err != nil {
		log.Warn("failed to iconv  into %v", i.config.Charset)
		return errors.New(fmt.Sprintf("failed to encode  into %v", i.config.Charset))
	}

	e.Fill(e.Meta(), e.Header(), bytes)
	return nil
}
