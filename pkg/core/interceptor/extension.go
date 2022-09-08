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

package interceptor

import (
	"github.com/loggie-io/loggie/pkg/core/api"
)

const DefaultOrder = 999

type Extension interface {
	Order() int
	BelongTo() (componentTypes []string)
	IgnoreRetry() bool
}

type ExtensionConfig struct {
	Order       int      `yaml:"order,omitempty" default:"900"`
	BelongTo    []string `yaml:"belongTo,omitempty"`
	IgnoreRetry *bool    `yaml:"ignoreRetry,omitempty" default:"true"`
}

type SortableInterceptor []api.Interceptor

func (si SortableInterceptor) Len() int {
	return len(si)
}

func (si SortableInterceptor) Less(i, j int) bool {
	i1 := si[i]
	i2 := si[j]
	var o1, o2 int
	{
		e1, ok := i1.(Extension)
		if ok {
			o1 = e1.Order()
		} else {
			o1 = DefaultOrder
		}
	}
	{
		e2, ok := i2.(Extension)
		if ok {
			o2 = e2.Order()
		} else {
			o2 = DefaultOrder
		}
	}
	return o1 < o2
}

func (si SortableInterceptor) Swap(i, j int) {
	si[i], si[j] = si[j], si[i]
}
