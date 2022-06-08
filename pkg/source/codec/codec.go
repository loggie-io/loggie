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

package codec

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
)

type SourceCodec interface {
	SetCodec(c Codec)
}

type Codec interface {
	Init()
	Decode(event api.Event) (api.Event, error)
}

type Factory func() Codec

var center = make(map[string]Factory)

func Register(name string, factory Factory) {
	_, ok := center[name]
	if ok {
		log.Panic("codec %s is duplicated", name)
	}

	center[name] = factory
}

func Get(name string) (Codec, bool) {
	f, ok := center[name]
	if !ok {
		return nil, ok
	}
	return f(), ok
}

var ProductFunc = func(productFunc api.ProductFunc, codec Codec) api.ProductFunc {
	return func(event api.Event) api.Result {
		if codec != nil {
			if _, err := codec.Decode(event); err != nil {
				log.Error("source codec decode failed: %v", err)
				// return fail would retry, ignore error here
			}
		}

		productFunc(event)
		return result.Success()
	}
}
