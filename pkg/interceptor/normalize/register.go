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
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
)

type Processor interface {
	Init(interceptor *Interceptor)
	Process(e api.Event) error
	GetName() string
}

type factory func() Processor

var registry = make(map[string]factory)

func register(name string, f factory) {
	_, ok := registry[name]
	if ok {
		log.Panic("processor %s is duplicated", name)
	}
	registry[name] = f
}

func getProcessor(name string) (Processor, bool) {
	trans, ok := registry[name]
	if !ok {
		return nil, false
	}
	return trans(), true
}
