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

const ProcessorUnderRoot = "underRoot"

type UnderRootProcessor struct {
	config      *UnderRootConfig
	interceptor *Interceptor
}

type UnderRootConfig struct {
	Keys []string `yaml:"keys,omitempty" validate:"required"`
}

func init() {
	register(ProcessorUnderRoot, func() Processor {
		return NewUnderRootProcessor()
	})
}

func NewUnderRootProcessor() *UnderRootProcessor {
	return &UnderRootProcessor{
		config: &UnderRootConfig{},
	}
}

func (r *UnderRootProcessor) Config() interface{} {
	return r.config
}

func (r *UnderRootProcessor) Init(interceptor *Interceptor) {
	r.interceptor = interceptor
}

func (r *UnderRootProcessor) GetName() string {
	return ProcessorAddMeta
}

func (r *UnderRootProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}
	if len(r.config.Keys) == 0 {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	obj := runtime.NewObject(header)
	for _, t := range r.config.Keys {
		upperPaths, key := runtime.GetQueryUpperPaths(t)
		upperVal := obj.GetPaths(upperPaths)
		val := upperVal.Get(key)
		if valMap, err := val.Map(); err == nil {
			for k, v := range valMap {
				obj.Set(k, v)
			}
		} else {
			log.Error("val.Map err:%s", err)
			r.interceptor.reportMetric(r)
			obj.Set(key, val)
		}
		upperVal.Del(key)
	}

	return nil
}
