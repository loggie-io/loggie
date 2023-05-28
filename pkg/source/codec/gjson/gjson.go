/*
Copyright 2022 Loggie Authors

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

package gjson

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/codec"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

const (
	Type = "gjson"
)

type GJson struct {
	config *Config
}

type Config struct {
	BodyFields string `yaml:"bodyFields,omitempty" validate:"required"` // use the fields as `Body`
	Prune      *bool  `yaml:"prune,omitempty"`                          // we drop all the fields except `Body` in default
}

func init() {
	codec.Register(Type, makeGJsonCodec)
}

func makeGJsonCodec() codec.Codec {
	return NewGJson()
}

func NewGJson() *GJson {
	return &GJson{
		config: &Config{},
	}
}

func (g *GJson) Config() interface{} {
	return g.config
}

func (g *GJson) Init() {
}

func (g *GJson) Decode(e api.Event) (api.Event, error) {
	if len(e.Body()) == 0 {
		return e, nil
	}
	// if enable prune, we only keep the fields in BodyFields
	if g.config.Prune == nil || *g.config.Prune == true {
		rawResult := gjson.ParseBytes(e.Body())
		rawBody := rawResult.Get(g.config.BodyFields)
		if !rawBody.Exists() {
			return nil, errors.Errorf("target %s cannot found in header", g.config.BodyFields)
		}
		body, ok := rawBody.Value().(string)
		if !ok {
			return nil, errors.Errorf("source codec json: target %v value is not string", body)
		}
		if len(body) == 0 {
			return nil, errors.Errorf("source codec json: target %v value is empty", body)
		}
		e.Fill(e.Meta(), nil, []byte(body))
		return e, nil
	}
	// if disable prune,we should marshal the whole event to json\
	var header map[string]interface{}
	header = e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}
	_, ok := gjson.ParseBytes(e.Body()).Value().(map[string]interface{})
	if !ok {
		log.Error("source codec json: target %v value is not map[string]interface{}", header)
		log.Debug("body: %s", e.Body())
		return nil, errors.Errorf("source codec json: target %v value is not map[string]interface{}", header)
	}
	header = gjson.ParseBytes(e.Body()).Value().(map[string]interface{})
	body, err := getBytes(header, g.config.BodyFields)
	if len(body) == 0 {
		return e, nil
	}
	if err != nil {
		return e, err
	}
	delete(header, g.config.BodyFields)
	e.Fill(e.Meta(), header, body)
	return e, nil
}

func getBytes(header map[string]interface{}, key string) ([]byte, error) {
	target, ok := header[key]
	if !ok {
		return nil, errors.Errorf("target %s cannot found in header", key)
	}

	targetValStr, ok := target.(string)
	if !ok {
		return nil, errors.Errorf("source codec json: target %v value is not string", key)
	}

	return []byte(targetValStr), nil
}
