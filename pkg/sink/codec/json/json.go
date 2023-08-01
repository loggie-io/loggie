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

package json

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/loggie-io/loggie/pkg/core/api"
	eventer "github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/sink/codec"
)

var (
	json = jsoniter.ConfigFastest
)

type Json struct {
	config    *Config
	codecConf *codec.Config
}

const (
	Type = "json"
)

const tsLayout = "2006-01-02T15:04:05.000Z"

func init() {
	codec.Register(Type, makeJsonCodec)
}

func makeJsonCodec() codec.Codec {
	return NewJson()
}

func NewJson() *Json {
	return &Json{
		config: &Config{},
	}
}

func (j *Json) Config() interface{} {
	return j.config
}

func (j *Json) Init(config *codec.Config) {
	j.codecConf = config
}

func (j *Json) Encode(e api.Event) ([]byte, error) {
	header := e.Header()

	if header == nil {
		header = make(map[string]interface{})
	}
	if j.config.BeatsFormat {
		beatsFormat(e)
	} else if len(e.Body()) != 0 {
		// put body in header
		header[eventer.Body] = util.ByteToStringUnsafe(e.Body())
	}

	var result []byte
	var err error
	if j.config.Pretty {
		result, err = json.MarshalIndent(header, "", "    ")
	} else {
		result, err = json.Marshal(header)
	}
	if err != nil {
		return nil, err
	}

	if j.codecConf.PrintEvents {
		log.Info("[print events] %s", string(result))
	}

	return result, nil
}

func beatsFormat(e api.Event) {
	meta := e.Meta()
	header := e.Header()
	if meta != nil {
		if timestamp, exist := meta.Get(eventer.SystemProductTimeKey); exist {
			if t, ok := timestamp.(time.Time); ok {
				header["@timestamp"] = t.UTC().Format(tsLayout)
			}
		}
	}

	if len(e.Body()) != 0 {
		header["message"] = string(e.Body())
	}
}
