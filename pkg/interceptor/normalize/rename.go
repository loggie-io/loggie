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
	"loggie.io/loggie/pkg/core/log"
	"strings"
)

const ProcessorRename = "rename"

type RenameProcessor struct {
	config *RenameConfig
}

type RenameConfig struct {
	Convert []Convert `yaml:"target,omitempty"`
}

func init() {
	register(ProcessorRename, func() Processor {
		return NewRenameProcessor()
	})
}

func NewRenameProcessor() *RenameProcessor {
	return &RenameProcessor{
		config: &RenameConfig{},
	}
}

func (r *RenameProcessor) Config() interface{} {
	return r.config
}

func (r *RenameProcessor) Init() {
}

func (r *RenameProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	for _, convert := range r.config.Convert {
		from := convert.From
		if strings.HasPrefix(from, event.PrivateKeyPrefix) || strings.HasPrefix(from, event.SystemKeyPrefix) {
			continue
		}

		val, ok := header[from]
		if !ok {
			log.Info("rename fields from %s is not exist", from)
			log.Debug("rename event: %s", e.String())
			continue
		}

		delete(header, from)
		header[convert.To] = val
	}

	return nil
}
