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
	"github.com/pkg/errors"
	"loggie.io/loggie/pkg/core/api"
	eventer "loggie.io/loggie/pkg/core/event"
	"loggie.io/loggie/pkg/core/log"
	"reflect"
	"strings"
	"time"
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

		if isStruct(v) {
			m, err := structToMap(v)
			if err != nil {
				log.Warn("convert struct to map error: %v", err)
				continue
			}
			metaData[k] = m
			continue
		}

		metaData[k] = v
	}
	return metaData
}

func isStruct(v interface{}) bool {
	refVal := reflect.ValueOf(v)
	if refVal.Kind() == reflect.Ptr {
		refVal = refVal.Elem()
	}
	if refVal.Kind() == reflect.Struct && reflect.TypeOf(v) != reflect.TypeOf(time.Time{}) {
		return true
	}
	return false
}

func structToMap(v interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(&v)
	if err != nil {
		return nil, errors.Errorf("json marshal %s in meta error: %v", v, err)
	}
	var m map[string]interface{}
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, errors.Errorf("json unmarshal %s in meta error: %v", v, err)
	}
	return m, nil
}
