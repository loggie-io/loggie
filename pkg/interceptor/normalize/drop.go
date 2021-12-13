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
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/event"
	"strings"
)

const ProcessorDrop = "drop"

type DropProcessor struct {
	config *DropConfig
}

type DropConfig struct {
	Target []string `yaml:"target,omitempty" validate:"required"`
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

func (r *DropProcessor) Init() {
}

func (r *DropProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}
	if len(r.config.Target) == 0 {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}
	body := e.Body()

	for _, target := range r.config.Target {
		if strings.HasPrefix(target, event.PrivateKeyPrefix) || strings.HasPrefix(target, event.SystemKeyPrefix) {
			continue
		}

		if target == event.Body {
			body = []byte{}
		} else {
			delete(header, target)
		}
	}

	e.Fill(header, body)
	return nil
}
