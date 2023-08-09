/*
Copyright 2023 Loggie Authors

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
	"github.com/loggie-io/loggie/pkg/util/json/gojson"
	"github.com/loggie-io/loggie/pkg/util/json/jsoniter"
	"github.com/loggie-io/loggie/pkg/util/json/sonic"
	"github.com/loggie-io/loggie/pkg/util/json/std"
)

const (
	Decoderjsoniter  = "jsoniter"
	Decodersonic     = "sonic"
	Decoderstd       = "std"
	Decodergojson    = "go-json"
	defaultCoderName = "default"
)

var decoderSet = map[string]JSON{
	Decoderjsoniter: &jsoniter.Jsoniter{},
	Decodersonic:    &sonic.Sonic{},
	Decoderstd:      &std.Std{},
	Decodergojson:   &gojson.Gojson{},
}

func init() {
	for name, decoder := range decoderSet {
		Register(name, decoder)
	}
	Register(defaultCoderName, &jsoniter.Jsoniter{})
}

type JSON interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
	MarshalIndent(v interface{}, prefix, indent string) ([]byte, error)
	MarshalToString(v interface{}) (string, error)
}

var JSONFactory = make(map[string]JSON)

func Register(name string, factory JSON) {
	JSONFactory[name] = factory
}

func SetDefaultEngine(name string) {
	JSONFactory[defaultCoderName] = JSONFactory[name]
}

func Marshal(v interface{}) ([]byte, error) {
	return JSONFactory[defaultCoderName].Marshal(v)
}

func Unmarshal(data []byte, v interface{}) error {
	return JSONFactory[defaultCoderName].Unmarshal(data, v)
}

func MarshalIndent(v interface{}, prefix, indent string) ([]byte, error) {
	return JSONFactory[defaultCoderName].MarshalIndent(v, prefix, indent)
}

func MarshalToString(v interface{}) (string, error) {
	return JSONFactory[defaultCoderName].MarshalToString(v)
}
