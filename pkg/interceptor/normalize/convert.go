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
	"strconv"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/runtime"
)

const ProcessorConvert = "convert"

const (
	typeBoolean = "bool"
	typeInteger = "integer"
	typeFloat   = "float"
)

type ConvertProcessor struct {
	config      *ConvertConfig
	interceptor *Interceptor
}

type ConvertConfig struct {
	Convert []Convert `yaml:"convert,omitempty" validate:"required"`
}

func init() {
	register(ProcessorConvert, func() Processor {
		return NewConvertProcessor()
	})
}

func NewConvertProcessor() *ConvertProcessor {
	return &ConvertProcessor{
		config: &ConvertConfig{},
	}
}

func (p *ConvertProcessor) Config() interface{} {
	return p.config
}

func (p *ConvertProcessor) Init(interceptor *Interceptor) {
	p.interceptor = interceptor
	log.Info("format: %v", p.config.Convert)
}

func (p *ConvertProcessor) GetName() string {
	return ProcessorConvert
}

func (p *ConvertProcessor) Process(e api.Event) error {
	if p.config == nil {
		return nil
	}

	header := e.Header()
	if header == nil {
		return nil
	}

	for _, convert := range p.config.Convert {
		obj := runtime.NewObject(header)
		srcVal := obj.GetPath(convert.From)
		if srcVal.IsNull() {
			log.Info("convert field %s not exist", convert.From)
			continue
		}

		val, err := srcVal.String()
		if err != nil {
			p.interceptor.reportMetric(p)
			log.Info("cannot parse %s into string in event: %s", convert.From, e.String())
			continue
		}

		obj.DelPath(convert.From)
		obj.SetPath(convert.From, p.format(val, convert.To))
	}

	return nil
}

func (p *ConvertProcessor) format(srcVal, dstFormat string) interface{} {
	switch dstFormat {
	case typeBoolean:
		dstVal, err := strconv.ParseBool(srcVal)
		if err != nil {
			log.Warn("format error %s", err)
			p.interceptor.reportMetric(p)
			goto original
		}
		return dstVal
	case typeInteger:
		dstVal, err := strconv.ParseInt(srcVal, 10, 64)
		if err != nil {
			log.Warn("format error %s", err)
			p.interceptor.reportMetric(p)
			goto original
		}
		return dstVal
	case typeFloat:
		dstVal, err := strconv.ParseFloat(srcVal, 64)
		if err != nil {
			log.Warn("format error %s", err)
			p.interceptor.reportMetric(p)
			goto original
		}
		return dstVal
	}

original:
	return srcVal
}
