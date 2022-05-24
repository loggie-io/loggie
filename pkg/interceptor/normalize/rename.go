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

const ProcessorMove = "rename"

type MoveProcessor struct {
	config       *MoveConfig
	pipelineName string
}

type MoveConfig struct {
	Convert []Convert `yaml:"convert,omitempty"`
}

func init() {
	register(ProcessorMove, func() Processor {
		return NewMoveProcessor()
	})
}

func NewMoveProcessor() *MoveProcessor {
	return &MoveProcessor{
		config: &MoveConfig{},
	}
}

func (r *MoveProcessor) Config() interface{} {
	return r.config
}

func (r *MoveProcessor) Init(pipeline string) {
}

func (r *MoveProcessor) GetPipeLine() string {
	return r.pipelineName
}

func (r *MoveProcessor) GetName() string {
	return ProcessorMove
}

func (r *MoveProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	for _, convert := range r.config.Convert {
		from := convert.From

		obj := runtime.NewObject(header)
		val := obj.GetPath(from)
		if val.IsNull() {
			log.Info("move fields from %s is not exist", from)
			log.Debug("move event: %s", e.String())
			continue
		}
		obj.DelPath(from)
		obj.SetPath(convert.To, val.Value())
	}

	return nil
}
