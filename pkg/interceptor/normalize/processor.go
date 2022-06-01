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
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/pkg/errors"
)

type ProcessorGroup struct {
	processor []Processor
}

func NewProcessorGroup(config ProcessorConfig) *ProcessorGroup {
	if len(config) == 0 {
		return &ProcessorGroup{}
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

	return &ProcessorGroup{
		processor: processors,
	}
}

func newProcessor(name string, properties cfg.CommonCfg) (Processor, error) {
	proc, ok := getProcessor(name)
	if !ok {
		return nil, errors.Errorf("processor %s cannot be found", name)
	}
	if c, ok := proc.(api.Config); ok {
		if properties == nil {
			properties = cfg.NewCommonCfg()
		}
		err := cfg.UnpackDefaultsAndValidate(properties, c.Config())
		if err != nil {
			return nil, errors.WithMessagef(err, "unpack processor %s config", name)
		}
	}
	return proc, nil
}

func (t *ProcessorGroup) InitAll(interceptor *Interceptor) {
	for _, p := range t.processor {
		p.Init(interceptor)
	}
}

func (t *ProcessorGroup) ProcessAll(e api.Event) error {
	for _, p := range t.processor {
		err := p.Process(e)
		if err != nil {
			return err
		}
	}
	return nil
}

func LogErrorWithIgnore(ignore bool, format string, a ...interface{}) {
	if !ignore {
		log.Error(format, a...)
	}
}
