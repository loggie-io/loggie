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
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/runtime"
)

const ProcessorTimestamp = "timestamp"

const typeString = "string"

type TimestampProcessor struct {
	config      *TimestampConfig
	interceptor *Interceptor
}

type TimestampConfig struct {
	Convert []TimestampConvert `yaml:"convert,omitempty"`
}

type TimestampConvert struct {
	From       string `yaml:"from,omitempty" validate:"required"`
	FromLayout string `yaml:"fromLayout,omitempty" validate:"required"`
	ToLayout   string `yaml:"toLayout,omitempty" validate:"required"`
	ToType     string `yaml:"toType,omitempty"`
	local      bool   `yaml:"local,omitempty"`
}

func init() {
	register(ProcessorTimestamp, func() Processor {
		return NewTimestampProcessor()
	})
}

func NewTimestampProcessor() *TimestampProcessor {
	return &TimestampProcessor{
		config: &TimestampConfig{},
	}
}

func (r *TimestampProcessor) Config() interface{} {
	return r.config
}

func (r *TimestampProcessor) Init(interceptor *Interceptor) {
	r.interceptor = interceptor
}

func (r *TimestampProcessor) GetName() string {
	return ProcessorTimestamp
}

func (r *TimestampProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}
	if len(r.config.Convert) == 0 {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	obj := runtime.NewObject(header)

	for _, target := range r.config.Convert {
		// parse timestamp
		timeStr, err := obj.GetPath(target.From).String()
		if err != nil {
			log.Info("unexpected type for timestamp in event: %s", e.String())
			r.interceptor.reportMetric(r)
			continue
		}
		if timeStr == "" {
			log.Info("cannot find %s in event: %s", target.From, e.String())
			continue
		}

		timeVal, err := time.Parse(target.FromLayout, timeStr)
		if err != nil {
			log.Info("parse time: %s by layout %s error", timeStr, target.FromLayout)
			r.interceptor.reportMetric(r)
			continue
		}

		if target.local {
			timeVal = timeVal.Local()
		}

		switch target.ToLayout {
		case "unix":
			s := timeVal.Unix()
			if target.ToType == typeString {
				obj.SetPath(target.From, strconv.FormatInt(s, 10))
			} else {
				obj.SetPath(target.From, s)
			}
		case "unix_ms":
			ms := timeVal.UnixNano() / int64(time.Millisecond)
			if target.ToType == typeString {
				obj.SetPath(target.From, strconv.FormatInt(ms, 10))
			} else {
				obj.SetPath(target.From, ms)
			}

		default:
			timeRes := timeVal.Format(target.ToLayout)
			obj.SetPath(target.From, timeRes)
		}
	}

	return nil
}
