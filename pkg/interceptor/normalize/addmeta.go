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
	eventer "loggie.io/loggie/pkg/core/event"
	"strings"
)

const ProcessorAddMeta = "addMeta"

type AddMetaProcessor struct {
	config *AddMetaConfig
}

type AddMetaConfig struct {
	Target string `yaml:"target,omitempty" default:"meta"`
}

func init() {
	register(ProcessorAddMeta, func() Processor {
		return NewAddMetaProcessor()
	})
}

func NewAddMetaProcessor() *AddMetaProcessor {
	return &AddMetaProcessor{
		config: &AddMetaConfig{},
	}
}

func (r *AddMetaProcessor) Config() interface{} {
	return r.config
}

func (r *AddMetaProcessor) Init() {
}

func (r *AddMetaProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	header[r.config.Target] = getMetaData(e)

	return nil
}

func getMetaData(e api.Event) map[string]interface{} {
	meta := e.Meta()
	if meta == nil {
		return nil
	}

	metaData := make(map[string]interface{})
	for k, v := range e.Meta().GetAll() {
		// ignore @private meta fields
		if strings.HasPrefix(k, eventer.PrivateKeyPrefix) {
			continue
		}

		metaData[k] = v
	}
	return metaData
}
