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
	"loggie.io/loggie/pkg/core/log"
	"strconv"
	"time"
)

const ProcessorTimestamp = "timestamp"

const typeString = "string"

type TimestampProcessor struct {
	config *TimestampConfig
}

type TimestampConfig struct {
	Target []TimestampConvert `yaml:"target,omitempty"`
}

type TimestampConvert struct {
	From       string `yaml:"from,omitempty" validate:"required"`
	FromLayout string `yaml:"fromLayout,omitempty" validate:"required"`
	ToLayout   string `yaml:"toLayout,omitempty" validate:"required"`
	ToType     string `yaml:"toType,omitempty"`
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

func (r *TimestampProcessor) Init() {
}

func (r *TimestampProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}
	if len(r.config.Target) == 0 {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	for _, target := range r.config.Target {
		// parse timestamp
		timeInHeader, ok := header[target.From]
		if !ok {
			log.Info("cannot find target.From %s fields in event: %s", target.From, e.String())
			continue
		}
		timeStr, ok := timeInHeader.(string)
		if !ok {
			log.Info("unexpected type for timestamp in event: %s", e.String())
			continue
		}
		timeVal, err := time.Parse(target.FromLayout, timeStr)
		if err != nil {
			log.Info("parse time: %s by layout %s error", timeStr, target.FromLayout)
			continue
		}

		switch target.ToLayout {
		case "unix":
			s := timeVal.Unix()
			if target.ToType == typeString {
				header[target.From] = strconv.FormatInt(s, 10)
			} else {
				header[target.From] = s
			}
		case "unix_ms":
			ms := timeVal.UnixNano() / int64(time.Millisecond)
			if target.ToType == typeString {
				header[target.From] = strconv.FormatInt(ms, 10)
			} else {
				header[target.From] = ms
			}

		default:
			timeRes := timeVal.Format(target.ToLayout)
			header[target.From] = timeRes
		}
	}

	return nil
}
