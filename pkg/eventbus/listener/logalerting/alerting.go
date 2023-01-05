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
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/eventbus/export/alertmanager"
)

const name = "logAlert"

func init() {
	eventbus.Registry(name, makeListener, eventbus.WithTopic(eventbus.LogAlertTopic), eventbus.WithTopic(eventbus.AlertTempTopic))
}

func makeListener() eventbus.Listener {
	l := &Listener{
		config: &Config{},
		done:   make(chan struct{}),
	}
	return l
}

type Config struct {
	Addr         []string          `yaml:"addr,omitempty" validate:"required"`
	BufferSize   int               `yaml:"bufferSize,omitempty" default:"100"`
	BatchTimeout time.Duration     `yaml:"batchTimeout,omitempty" default:"10s"`
	BatchSize    int               `yaml:"batchSize,omitempty" default:"10"`
	Template     *string           `yaml:"template,omitempty"`
	Timeout      int               `yaml:"timeout,omitempty" default:"30"`
	Headers      map[string]string `yaml:"headers,omitempty"`
	Method       string            `yaml:"method,omitempty"`
	LineLimit    int               `yaml:"lineLimit,omitempty" default:"10"`
}

type Listener struct {
	config *Config
	done   chan struct{}

	bufferChan chan *eventbus.Event

	SendBatch []*eventbus.Event

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
	l.SendBatch = make([]*eventbus.Event, 0)

	l.alertCli = alertmanager.NewAlertManager(l.config.Addr, l.config.Timeout, l.config.LineLimit, l.config.Template, l.config.Headers, l.config.Method)

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

func (l *Listener) process(event *eventbus.Event) {
	log.Debug("process event %s", event)
	if event.Topic == eventbus.AlertTempTopic {
		s, ok := event.Data.(string)
		if ok {
			l.alertCli.UpdateTemp(s)
		}

	}

	l.SendBatch = append(l.SendBatch, event)

	if len(l.SendBatch) >= l.config.BatchSize {
		l.flush()
	}
}

func (l *Listener) flush() {
	if len(l.SendBatch) == 0 {
		return
	}

	events := make([]*eventbus.Event, len(l.SendBatch))
	copy(events, l.SendBatch)
	l.alertCli.SendAlert(events)

	l.SendBatch = l.SendBatch[:0]

}
