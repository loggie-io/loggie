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

package json

import (
	"github.com/loggie-io/loggie/pkg/source/codec"
	"github.com/pkg/errors"
)

const (
	Type = "json"
)

type Json struct {
	config *Config
}

type Config struct {
	BodyFields string `yaml:"bodyFields,omitempty" validate:"required"` // use the fields as `Body`
	Prune      *bool  `yaml:"prune,omitempty"`                          // we drop all the fields except `Body` in default
}

func init() {
	codec.Register(Type, makeJsonCodec)
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

func pruneCLRF(in []byte) []byte {
	var out []byte

	length := len(in) - 1
	if length >= 2 && in[length] == '\n' && in[length-1] == '\r' {
		out = in[:length-1]
	} else if in[length] == '\n' {
		out = in[:length]
	} else {
		out = in
	}

	return out
}
