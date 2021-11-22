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
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/util"
	"strconv"
	"time"
)

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
	register("timestamp", func() Processor {
		return NewTimestampProcessor()
	})
}

func NewTimestampProcessor() *TimestampProcessor {
	return &TimestampProcessor{
		config: &TimestampConfig{},
	}
}

func (d *TimestampProcessor) Config() interface{} {
	return d.config
}

func (d *TimestampProcessor) Process(jsonObj *simplejson.Json) {
	if d.config == nil {
		return
	}
	for _, target := range d.config.Target {
		paths := util.GetQueryPaths(target.From)
		// parse timestamp
		timeStr, err := jsonObj.GetPath(paths...).String()
		if err != nil {
			log.Warn("unexpected type for timestamp, err: %+v", err)
			return
		}

		timeVal, err := time.Parse(target.FromLayout, timeStr)
		if err != nil {
			log.Warn("parse time: %s by layout %s error", timeStr, target.FromLayout)
			return
		}

		switch target.ToLayout {
		case "unix":
			s := timeVal.Unix()
			if target.ToType == "string" {
				jsonObj.SetPath(paths, strconv.FormatInt(s, 10))
			} else {
				jsonObj.SetPath(paths, s)
			}
		case "unix_ms":
			ms := timeVal.UnixNano() / int64(time.Millisecond)
			if target.ToType == "string" {
				jsonObj.SetPath(paths, strconv.FormatInt(ms, 10))
			} else {
				jsonObj.SetPath(paths, ms)
			}

		default:
			timeRes := timeVal.Format(target.ToLayout)
			jsonObj.SetPath(paths, timeRes)
		}
	}
}
