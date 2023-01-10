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

package error

import (
	"fmt"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

const Type = "error"

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	return &LoggieError{
		pipelineName: info.PipelineName,
		config:       &Config{},
		stop:         make(chan struct{}),
		eventPool:    info.EventPool,
	}
}

type LoggieError struct {
	pipelineName string
	name         string
	config       *Config
	stop         chan struct{}
	eventChan    chan string
	eventPool    *event.Pool
	listener     *Listener
	subscribe    *eventbus.Subscribe
}

func (d *LoggieError) Config() interface{} {
	return d.config
}

func (d *LoggieError) Category() api.Category {
	return api.SOURCE
}

func (d *LoggieError) Type() api.Type {
	return Type
}

func (d *LoggieError) String() string {
	return fmt.Sprintf("%s/%s", api.SOURCE, Type)
}

func (d *LoggieError) Init(context api.Context) error {
	d.name = context.Name()
	d.listener = &Listener{
		name:   fmt.Sprintf("%s/%s", d.pipelineName, name),
		source: d,
		done:   make(chan struct{}),
	}

	d.stop = make(chan struct{})
	d.eventChan = make(chan string)

	d.subscribe = eventbus.RegistryTemporary(d.listener.name, func() eventbus.Listener {
		return d.listener
	}, eventbus.WithTopic(eventbus.ErrorTopic))
	return nil
}

func (d *LoggieError) Start() error {
	_ = d.listener.Start()
	if d.config.ErrorTestEnabled {
		go d.runTest()
	}
	return nil
}

func (d *LoggieError) Stop() {
	eventbus.UnRegistrySubscribeTemporary(d.subscribe)
	d.listener.Stop()
	close(d.stop)
}

func (d *LoggieError) runTest() {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
			log.Error("test error %s", time.Now().String())
		}
	}
}

func (d *LoggieError) ProductLoop(productFunc api.ProductFunc) {

	for {
		select {
		case <-d.stop:
			return

		case message := <-d.eventChan:
			e := d.eventPool.Get()
			event.ErrorIntoEvent(e, message)
			header := e.Header()
			if len(d.config.Additions) > 0 {
				header[event.Addition] = d.config.Additions
			}
			if len(d.config.Fields) > 0 {
				header[event.Fields] = d.config.Fields
			}
			e.Fill(e.Meta(), header, e.Body())
			productFunc(e)
		}
	}
}

func (d *LoggieError) consumeEvent(message string) {
	select {
	case d.eventChan <- message:
	default:
	}
}

func (d *LoggieError) Commit(events []api.Event) {
	d.eventPool.PutAll(events)
}
