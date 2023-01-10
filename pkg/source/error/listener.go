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
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
)

const name = "errorSourceListener"

type Listener struct {
	name       string
	source     *LoggieError
	bufferChan chan *eventbus.Event
	done       chan struct{}
}

func (l *Listener) Init(context api.Context) error {
	return nil
}

func (l *Listener) Name() string {
	return l.name
}

func (l *Listener) Config() interface{} {
	return nil
}

func (l *Listener) Start() error {
	l.bufferChan = make(chan *eventbus.Event, 0)
	log.Info("starting alertWebhook listener %s", l.name)
	go l.run()
	return nil
}

func (l *Listener) Stop() {
	log.Info("stopping alertWebhook listener %s", l.name)
	close(l.done)
}

func (l *Listener) Subscribe(event eventbus.Event) {
	select {
	case l.bufferChan <- &event:
	default:
	}

}

func (l *Listener) run() {
	for {
		select {
		case <-l.done:
			return

		case d := <-l.bufferChan:
			l.process(d)
		}
	}
}

func (l *Listener) process(e *eventbus.Event) {
	errorData, ok := e.Data.(eventbus.ErrorMetricData)
	if !ok {
		log.Warn("fail to convert loggie error to event, ignore...")
		return
	}
	l.source.consumeEvent(errorData.ErrorMsg)
}
