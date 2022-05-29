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

package normalize

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/util/runtime"
)

const ProcessorAdd = "add"

type AddProcessor struct {
	config      *AddConfig
	interceptor *Interceptor
}

type AddConfig struct {
	Fields map[string]interface{} `yaml:"fields,omitempty" validate:"required"`
}

func init() {
	register(ProcessorAdd, func() Processor {
		return NewAddProcessor()
	})
}

func NewAddProcessor() *AddProcessor {
	return &AddProcessor{
		config: &AddConfig{},
	}
}

func (r *AddProcessor) Config() interface{} {
	return r.config
}

func (r *AddProcessor) GetName() string {
	return ProcessorAdd
}

func (r *AddProcessor) Init(interceptor *Interceptor) {
	r.interceptor = interceptor
}

func (r *AddProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}
	if len(r.config.Fields) == 0 {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}
	for k, v := range r.config.Fields {
		obj := runtime.NewObject(header)
		obj.SetPath(k, v)
	}

	return nil
}
