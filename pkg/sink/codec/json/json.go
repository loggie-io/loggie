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
	"github.com/bitly/go-simplejson"
	jsoniter "github.com/json-iterator/go"
	"loggie.io/loggie/pkg/core/api"
	eventer "loggie.io/loggie/pkg/core/event"
	"loggie.io/loggie/pkg/sink/codec"
	"loggie.io/loggie/pkg/sink/codec/transformer"
	"loggie.io/loggie/pkg/source/file"
	"strings"
)

var (
	json = jsoniter.ConfigFastest
)

type Json struct {
	config    *Config
	transform *transformer.Transformer
}

const Type = "json"

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

func (j *Json) Init() {
	j.transform = transformer.NewTransformer(j.config.Transformer)
}

func (j *Json) Encode(event api.Event) (*codec.Result, error) {
	h := event.Header()

	// events should not be modified
	header := make(map[string]interface{})
	if j.config.BeatsFormat {
		if sysState, ok := h[file.SystemStateKey]; ok {
			stat, ok := sysState.(*file.State)
			if ok {
				header["@timestamp"] = stat.CollectTime.Format(tsLayout)
			}
		}

		if len(event.Body()) != 0 {
			header["message"] = string(event.Body())
		}
	} else {
		// put body in header
		if len(event.Body()) != 0 {
			header["body"] = string(event.Body())
		}
	}

	for k, v := range h {
		if strings.HasPrefix(k, eventer.PrivateKeyPrefix) {
			continue
		}

		if j.config.Prune && strings.HasPrefix(k, eventer.SystemKeyPrefix) {
			continue
		}

		header[k] = v
	}

	// format
	if j.config.Transformer == nil {
		var err error
		var out []byte
		if j.config.Pretty {
			out, err = json.MarshalIndent(header, "", "    ")
		} else {
			out, err = json.Marshal(header)
		}
		if err != nil {
			return nil, err
		}

		result := &codec.Result{
			Raw: out,
			Lookup: func(paths ...string) (interface{}, error) {
				jsonObj, err := simplejson.NewJson(out)
				if err != nil {
					return nil, err
				}
				return jsonObj.GetPath(paths...).String()
			},
		}
		return result, nil
	}

	out, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	jsonObj, err := simplejson.NewJson(out)
	if err != nil {
		return nil, err
	}

	j.transform.Transform(jsonObj)

	if j.config.Pretty {
		out, err = jsonObj.EncodePretty()
	} else {
		out, err = jsonObj.Encode()
	}
	if err != nil {
		return nil, err
	}
	result := &codec.Result{
		Raw: out,
		Lookup: func(paths ...string) (interface{}, error) {
			return jsonObj.GetPath(paths...).String()
		},
	}
	return result, nil
}
