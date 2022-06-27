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
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/util/runtime"
)

const ProcessorDrop = "drop"

type DropProcessor struct {
	config      *DropConfig
	interceptor *Interceptor
}

type DropConfig struct {
	Targets []string `yaml:"targets,omitempty" validate:"required"`
}

func init() {
	register(ProcessorDrop, func() Processor {
		return NewDropProcessor()
	})
}

func NewDropProcessor() *DropProcessor {
	return &DropProcessor{
		config: &DropConfig{},
	}
}

func (r *DropProcessor) Config() interface{} {
	return r.config
}

func (r *DropProcessor) Init(interceptor *Interceptor) {
	r.interceptor = interceptor
}

func (r *DropProcessor) GetName() string {
	return ProcessorDrop
}

func (r *DropProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}
	if len(r.config.Targets) == 0 {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}
	body := e.Body()

	for _, target := range r.config.Targets {
		if target == event.Body {
			body = []byte{}
		} else {
			obj := runtime.NewObject(e.Header())
			obj.DelPath(target)
		}
	}

	e.Fill(e.Meta(), header, body)
	return nil
}
