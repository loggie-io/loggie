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

package webhook

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/batch"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
)

const name = "webhookListener"

type Listener struct {
	sink       *Sink
	bufferChan chan *eventbus.Event
	done       chan struct{}
}

func (l *Listener) Init(context api.Context) error {
	return nil
}

func (l *Listener) Name() string {
	return name
}

func (l *Listener) Config() interface{} {
	return nil
}

func (l *Listener) Start() error {
	l.bufferChan = make(chan *eventbus.Event, 0)
	log.Info("starting webhook listener")
	go l.run()
	return nil
}

func (l *Listener) Stop() {
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

func (l *Listener) process(event *eventbus.Event) {
	data, ok := event.Data.(*api.Event)
	if !ok {
		log.Info("fail to convert data to event")
		return
	}

	events := []api.Event{*data}
	batchWithEvent := batch.NewBatchWithEvents(events)
	l.sink.Consume(batchWithEvent)
	batchWithEvent.Release()
}
