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
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/runtime"
)

const ProcessorCopy = "copy"

type CopyProcessor struct {
	config      *CopyConfig
	interceptor *Interceptor
}

type CopyConfig struct {
	Convert []Convert `yaml:"convert,omitempty" validate:"required"`
}

func init() {
	register(ProcessorCopy, func() Processor {
		return NewCopyProcessor()
	})
}

func NewCopyProcessor() *CopyProcessor {
	return &CopyProcessor{
		config: &CopyConfig{},
	}
}

func (r *CopyProcessor) Config() interface{} {
	return r.config
}

func (r *CopyProcessor) Init(interceptor *Interceptor) {
	r.interceptor = interceptor
}

func (r *CopyProcessor) GetName() string {
	return ProcessorCopy
}

func (r *CopyProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	for _, c := range r.config.Convert {
		src := c.From

		obj := runtime.NewObject(header)
		val := obj.GetPath(src)
		if val.IsNull() {
			log.Info("copy fields from %s is not exist", src)
			log.Debug("copy event: %s", e.String())
			continue
		}
		obj.SetPath(c.To, val.Value())
	}

	return nil
}
