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

package sink

import (
	"fmt"
	"sort"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
)

type Interceptor interface {
	api.Interceptor
	Intercept(invoker Invoker, invocation Invocation) api.Result
}

type AbstractInterceptor struct {
	DoName      func() string
	DoIntercept func(invoker Invoker, invocation Invocation) api.Result
}

func (ai *AbstractInterceptor) Intercept(invoker Invoker, invocation Invocation) api.Result {
	return ai.DoIntercept(invoker, invocation)
}

func (ai *AbstractInterceptor) Init(context api.Context) {
	// ignore
}

func (ai *AbstractInterceptor) Start() {
	// ignore
}

func (ai *AbstractInterceptor) Stop() {
	// ignore
}

func (ai *AbstractInterceptor) Category() api.Category {
	return api.INTERCEPTOR
}

func (ai *AbstractInterceptor) Type() api.Type {
	return api.Type(ai.DoName())
}

func (ai *AbstractInterceptor) String() string {
	return fmt.Sprintf("%s/%s", ai.Category(), ai.Type())
}

func (ai *AbstractInterceptor) Config() interface{} {
	return nil
}

type SortableInterceptor []Interceptor

func (si SortableInterceptor) Len() int {
	return len(si)
}

func (si SortableInterceptor) Less(i, j int) bool {
	i1 := si[i]
	i2 := si[j]
	var o1, o2 int
	{
		e1, ok := i1.(interceptor.Extension)
		if ok {
			o1 = e1.Order()
		} else {
			o1 = interceptor.DefaultOrder
		}
	}
	{
		e2, ok := i2.(interceptor.Extension)
		if ok {
			o2 = e2.Order()
		} else {
			o2 = interceptor.DefaultOrder
		}
	}
	return o1 < o2
}

func (si SortableInterceptor) Swap(i, j int) {
	si[i], si[j] = si[j], si[i]
}

func (si SortableInterceptor) Sort() {
	sort.Sort(si)
}
