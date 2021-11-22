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

package transformer

import (
	"github.com/bitly/go-simplejson"
	"github.com/pkg/errors"
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/cfg"
	"loggie.io/loggie/pkg/core/log"
)

type Transformer struct {
	processor []Processor
}

func NewTransformer(config TransConfig) *Transformer {
	if len(config) == 0 {
		return &Transformer{}
	}

	var processors []Processor
	for _, conf := range config {
		for name, properties := range conf {
			proc, err := newProcessor(name, properties)
			if err != nil {
				log.Warn("get processor error: %+v", err)
				continue
			}
			processors = append(processors, proc)
		}
	}

	return &Transformer{
		processor: processors,
	}
}

func newProcessor(name string, properties cfg.CommonCfg) (Processor, error) {
	proc, ok := getProcessor(name)
	if !ok {
		return nil, errors.Errorf("transformer %s cannot be found", name)
	}
	if c, ok := proc.(api.Config); ok {
		err := cfg.UnpackAndDefaults(properties, c.Config())
		if err != nil {
			return nil, errors.WithMessagef(err, "unpack transformer %s config", name)
		}
	}
	return proc, nil
}

func (t *Transformer) Transform(jsonObj *simplejson.Json) {
	for _, p := range t.processor {
		p.Process(jsonObj)
	}
}
