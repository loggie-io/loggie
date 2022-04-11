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
	config *FmtConfig

	patternMatcher map[string][][]string
}

type FmtConfig struct {
	Fields map[string]string `yaml:"fields,omitempty" validate:"required"`
}

func (c *FmtConfig) Validate() error {
	for _, v := range c.Fields {
		if _, err := pattern.InitMatcher(v); err != nil {
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
		patternMatcher: make(map[string][][]string),
	}
}

func (r *FmtProcessor) Config() interface{} {
	return r.config
}

func (r *FmtProcessor) Init() {
	for k, v := range r.config.Fields {
		matcher := pattern.MustInitMatcher(v)
		r.patternMatcher[k] = matcher
	}
}

func (r *FmtProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}
	if len(r.config.Fields) == 0 {
		return nil
	}

	header := e.Header()
	headerObj := runtime.NewObject(header)

	for k, v := range r.config.Fields {
		result, err := runtime.PatternFormat(headerObj, v, r.patternMatcher[k])
		if err != nil {
			log.Warn("reformat %s by %s error: %v", k, v, err)
			continue
		}

		headerObj.SetPath(k, result)
	}

	return nil
}
