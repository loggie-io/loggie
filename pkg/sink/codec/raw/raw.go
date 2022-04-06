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

package raw

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/sink/codec"
)

const (
	Type = "raw"
)

func init() {
	codec.Register(Type, makeRawCodec)
}

type Raw struct {
}

func makeRawCodec() codec.Codec {
	return NewRaw()
}

func NewRaw() *Raw {
	return &Raw{}
}

func (j *Raw) Init() {
}

func (j *Raw) Encode(e api.Event) ([]byte, error) {
	return e.Body(), nil
}
