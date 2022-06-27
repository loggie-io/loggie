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

package normalize

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/loggie-io/loggie/pkg/util/runtime"
)

const ProcessorFmt = "fmt"

type FmtProcessor struct {
	config         *FmtConfig
	interceptor    *Interceptor
	patternMatcher map[string]*pattern.Pattern
}

type FmtConfig struct {
	Fields map[string]string `yaml:"fields,omitempty" validate:"required"`
}

func (c *FmtConfig) Validate() error {
	for _, v := range c.Fields {
		if err := pattern.Validate(v); err != nil {
			return err
		}
	}
	return nil
}

func init() {
	register(ProcessorFmt, func() Processor {
		return NewFmtProcessor()
	})
}

func NewFmtProcessor() *FmtProcessor {
	return &FmtProcessor{
		config:         &FmtConfig{},
		patternMatcher: make(map[string]*pattern.Pattern),
	}
}

func (r *FmtProcessor) Config() interface{} {
	return r.config
}

func (r *FmtProcessor) Init(interceptor *Interceptor) {
	for k, v := range r.config.Fields {
		p, _ := pattern.Init(v)
		r.patternMatcher[k] = p
	}
	r.interceptor = interceptor
}

func (r *FmtProcessor) GetName() string {
	return ProcessorFmt
}

func (r *FmtProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}
	if len(r.config.Fields) == 0 {
		return nil
	}

	headerObj := runtime.NewObject(e.Header())

	for k, v := range r.config.Fields {
		result, err := r.patternMatcher[k].WithObject(headerObj).Render()
		if err != nil {
			log.Warn("reformat %s by %s error: %v", k, v, err)
			r.interceptor.reportMetric(r)
			continue
		}

		headerObj.SetPath(k, result)
	}

	return nil
}
