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

package pipeline

import (
	"fmt"
	"github.com/pkg/errors"
	"sync"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/spi"
)

var codeFactory = map[string]Factory{}

type Factory func(info Info) api.Component

func codeWithoutName(category api.Category, typename api.Type) string {
	return code(category, typename, "")
}

func code(category api.Category, typename api.Type, name string) string {
	return fmt.Sprintf("%s/%s/%s", category, typename, name)
}

func Register(category api.Category, typename api.Type, factory Factory) {
	code := codeWithoutName(category, typename)
	if codeFactory[code] != nil {
		panic(fmt.Errorf("component code '%s' exists already", code))
	}
	codeFactory[code] = factory
}

func GetWithType(category api.Category, typename api.Type, info Info) (api.Component, error) {
	code := codeWithoutName(category, typename)
	factory, ok := codeFactory[code]
	if !ok {
		return nil, fmt.Errorf("component code '%s' is not exists", code)
	}
	component := factory(info)
	if component.Type() != typename {
		return nil, fmt.Errorf("component with code '%s' type is '%s',not '%s'", code, component.Type(), typename)
	}

	return component, nil
}

type RegisterCenter struct {
	nameComponents map[string]api.Component
	nameListeners  map[string]spi.ComponentListener
	lock           sync.Mutex
}

func NewRegisterCenter() *RegisterCenter {
	return &RegisterCenter{
		nameComponents: make(map[string]api.Component),
		nameListeners:  make(map[string]spi.ComponentListener),
	}
}

func (r *RegisterCenter) load(code string) api.Component {
	component, ok := r.nameComponents[code]
	if !ok {
		panic(fmt.Sprintf("component[%s] is not exist", code))
	}
	return component
}

func (r *RegisterCenter) LoadSink(typename api.Type, name string) api.Sink {
	code := code(api.SINK, typename, name)
	component := r.load(code)
	if api.SINK != component.Category() {
		panic(fmt.Sprintf("component[%s] is not a sink", code))
	}
	return component.(api.Sink)
}

func (r *RegisterCenter) LoadQueue(typename api.Type, name string) api.Queue {
	code := code(api.QUEUE, typename, name)
	component := r.load(code)
	if api.QUEUE != component.Category() {
		panic(fmt.Sprintf("component[%s] is not a queue", code))
	}
	return component.(api.Queue)
}

func (r *RegisterCenter) LoadDefaultQueue() api.Queue {
	for _, v := range r.nameComponents {
		if v.Category() == api.QUEUE {
			return v.(api.Queue)
		}
	}
	return nil
}

func (r *RegisterCenter) LoadSource(typename api.Type, name string) api.Source {
	code := code(api.SOURCE, typename, name)
	component := r.load(code)
	if api.SOURCE != component.Category() {
		panic(fmt.Sprintf("component[%s] is not a source", code))
	}
	return component.(api.Source)
}

func (r *RegisterCenter) LoadInterceptor(typename api.Type, name string) api.Interceptor {
	code := code(api.INTERCEPTOR, typename, name)
	component := r.load(code)
	if api.INTERCEPTOR != component.Category() {
		panic(fmt.Sprintf("component[%s] is not a interceptor", code))
	}
	return component.(api.Interceptor)
}

func (r *RegisterCenter) LoadInterceptors() []api.Interceptor {
	components := make([]api.Interceptor, 0)
	for _, v := range r.nameComponents {
		if v.Category() == api.INTERCEPTOR {
			components = append(components, v)
		}
	}
	return components
}

func (r *RegisterCenter) LoadSelector(typename api.Type, name string) api.Selector {
	code := code(api.SELECTOR, typename, name)
	component := r.load(code)
	if api.SELECTOR != component.Category() {
		panic(fmt.Sprintf("component[%s] is not a selector", code))
	}
	return component.(api.Selector)
}

func (r *RegisterCenter) LoadWithType(typename api.Type, name string, componentType api.Category) api.Component {
	code := code(componentType, typename, name)
	component := r.load(code)
	if componentType != component.Category() {
		panic(fmt.Sprintf("component[%s] is not a %v", code, componentType))
	}
	return component
}

func (r *RegisterCenter) Register(component api.Component, name string) error {
	code := code(component.Category(), component.Type(), name)
	_, ok := r.nameComponents[code]
	if ok {
		return errors.Errorf("component[%s] is duplicated, type/name should be unique", code)
	}
	r.nameComponents[code] = component
	return nil
}

func (r *RegisterCenter) RegisterListener(listener spi.ComponentListener) {
	name := listener.Name()
	_, ok := r.nameListeners[name]
	if ok {
		log.Warn("component listener[%s] is exist", name)
		return
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	_, ok = r.nameListeners[name]
	if ok {
		return
	}
	r.nameListeners[name] = listener
}

func (r *RegisterCenter) LoadQueueListeners() []spi.QueueListener {
	qls := make([]spi.QueueListener, 0)
	for _, listener := range r.nameListeners {
		if queueListener, ok := listener.(spi.QueueListener); ok {
			qls = append(qls, queueListener)
		}
	}
	return qls
}

// get components by type
func (r *RegisterCenter) Components(category api.Category) []api.Component {
	components := make([]api.Component, 0)
	for _, v := range r.nameComponents {
		if v.Category() == category {
			components = append(components, v)
		}
	}
	return components
}

func (r *RegisterCenter) AllComponents() []api.Component {
	components := make([]api.Component, 0)
	for _, v := range r.nameComponents {
		components = append(components, v)
	}
	return components
}

func (r *RegisterCenter) Listeners() []spi.ComponentListener {
	listeners := make([]spi.ComponentListener, 0)
	for _, listener := range r.nameListeners {
		listeners = append(listeners, listener)
	}
	return listeners
}

func (r *RegisterCenter) removeComponent(typename api.Type, category api.Category, name string) {
	code := code(category, typename, name)
	delete(r.nameComponents, code)
}

func (r *RegisterCenter) cleanData() {
	r.nameComponents = nil
	r.nameListeners = nil
}
