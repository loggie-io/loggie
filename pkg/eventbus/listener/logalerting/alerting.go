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

package logalerting

import (
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/logalert"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/eventbus/export/alertmanager"
)

const name = "logAlert"

func init() {
	eventbus.Registry(name, makeListener, eventbus.WithTopic(eventbus.LogAlertTopic), eventbus.WithTopic(eventbus.AlertTempTopic),
		eventbus.WithTopic(eventbus.ErrorTopic), eventbus.WithTopic(eventbus.NoDataTopic))
}

func makeListener() eventbus.Listener {
	l := &Listener{
		config: &logalert.Config{},
		done:   make(chan struct{}),
	}
	return l
}

type Listener struct {
	config *logalert.Config
	done   chan struct{}

	bufferChan chan *eventbus.Event

	SendBatch []*api.Event

	alertCli *alertmanager.AlertManager
}

func (l *Listener) Init(context api.Context) error {
	return nil
}

func (l *Listener) Name() string {
	return name
}

func (l *Listener) Config() interface{} {
	return l.config
}

func (l *Listener) Start() error {
	l.bufferChan = make(chan *eventbus.Event, l.config.BufferSize)
	l.SendBatch = make([]*api.Event, 0)

	l.alertCli = alertmanager.NewAlertManager(l.config)

	log.Info("starting logAlert listener")
	go l.run()
	return nil
}

func (l *Listener) Stop() {
	close(l.done)
}

func (l *Listener) Subscribe(event eventbus.Event) {
	l.bufferChan <- &event
}

func (l *Listener) run() {
	timeout := time.NewTicker(l.config.BatchTimeout)
	defer timeout.Stop()

	for {
		select {
		case <-l.done:
			return

		case d := <-l.bufferChan:
			l.process(d)

		case <-timeout.C:
			l.flush()
		}
	}
}

func (l *Listener) process(e *eventbus.Event) {
	if e.Topic == eventbus.AlertTempTopic {
		l.processAlertTempTopic(e)
	} else if e.Topic == eventbus.ErrorTopic {
		l.processErrorTopic(e)
	} else if e.Topic == eventbus.NoDataTopic {
		l.processNoDataTopic(e)
	} else {
		l.processLogAlertTopic(e)
	}
}

func (l *Listener) processAlertTempTopic(e *eventbus.Event) {
	s, ok := e.Data.(string)
	if ok {
		l.alertCli.UpdateTemp(s)
	}
}

func (l *Listener) processErrorTopic(e *eventbus.Event) {
	if !l.config.SendLoggieError {
		return
	}

	errorData, ok := e.Data.(eventbus.ErrorMetricData)
	if !ok {
		log.Warn("fail to convert loggie error to event, ignore...")
		return
	}

	apiEvent := event.ErrorToEvent(errorData.ErrorMsg)
	if l.config.SendLoggieErrorAtOnce {
		l.alertCli.SendAlert([]*api.Event{apiEvent}, true)
		return
	}

	l.sendToBatch(apiEvent)
}

func (l *Listener) processLogAlertTopic(e *eventbus.Event) {
	apiEvent, ok := e.Data.(*api.Event)
	if !ok {
		log.Warn("fail to convert data to event, ignore...")
		return
	}

	if (*apiEvent).Header()[event.ReasonKey] == event.NoDataKey && l.config.SendNoDataAlertAtOnce {
		l.alertCli.SendAlert([]*api.Event{apiEvent}, true)
		return
	}

	l.sendToBatch(apiEvent)
}

func (l *Listener) processNoDataTopic(e *eventbus.Event) {
	apiEvent, ok := e.Data.(*api.Event)
	if !ok {
		log.Warn("fail to convert data to event, ignore...")
		return
	}

	if l.config.SendNoDataAlertAtOnce {
		l.alertCli.SendAlert([]*api.Event{apiEvent}, true)
		return
	}

	l.sendToBatch(apiEvent)
}

func (l *Listener) sendToBatch(e *api.Event) {
	l.SendBatch = append(l.SendBatch, e)

	if len(l.SendBatch) >= l.config.BatchSize {
		l.flush()
	}
}

func (l *Listener) flush() {
	if len(l.SendBatch) == 0 {
		return
	}

	events := make([]*api.Event, len(l.SendBatch))
	copy(events, l.SendBatch)
	l.alertCli.SendAlert(events, l.config.SendLogAlertAtOnce)

	l.SendBatch = l.SendBatch[:0]

}
