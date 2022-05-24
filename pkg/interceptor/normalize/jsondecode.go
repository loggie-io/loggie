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
	jsoniter "github.com/json-iterator/go"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/runtime"
)

const ProcessorJsonDecode = "jsonDecode"

var (
	json = jsoniter.ConfigFastest
)

type JsonDecodeProcessor struct {
	config       *JsonDecodeConfig
	pipelineName string
}

type JsonDecodeConfig struct {
	Target string `yaml:"target,omitempty" default:"body"`
}

func init() {
	register(ProcessorJsonDecode, func() Processor {
		return NewJsonDecodeProcessor()
	})
}

func NewJsonDecodeProcessor() *JsonDecodeProcessor {
	return &JsonDecodeProcessor{
		config: &JsonDecodeConfig{},
	}
}

func (r *JsonDecodeProcessor) Config() interface{} {
	return r.config
}

func (r *JsonDecodeProcessor) Init(pipeline string) {
}

func (r *JsonDecodeProcessor) GetPipeLine() string {
	return r.pipelineName
}

func (r *JsonDecodeProcessor) GetName() string {
	return ProcessorJsonDecode
}

func (r *JsonDecodeProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	var val []byte
	target := r.config.Target
	if target == event.Body {
		val = e.Body()
	} else {
		obj := runtime.NewObject(header)
		v, err := obj.GetPath(target).String()
		if err != nil {
			log.Warn("get content from %s failed %v", target, err)
			return nil
		}
		if v == "" {
			return nil
		}

		val = []byte(v)
	}

	res := make(map[string]interface{})
	err := json.Unmarshal(val, &res)
	if err != nil {
		log.Info("unmarshal data: %s err: %v", string(val), err)
		return nil
	}
	for k, v := range res {
		header[k] = v
	}

	return nil
}
