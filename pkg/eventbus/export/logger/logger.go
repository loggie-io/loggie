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

package logger

import (
	"encoding/json"
	"loggie.io/loggie/pkg/core/log"
	"time"
)

var lg = newLogger()

type Event struct {
	Topic string `json:"topic"`
	Data  []byte `json:"data"`
}

type Config struct {
	Enabled bool          `yaml:"enabled"`
	Period  time.Duration `yaml:"period" default:"10s"`
}

type logger struct {
	data map[string]map[string]interface{} // key=topic

	eventChan chan *Event

	done chan struct{}
}

func newLogger() *logger {
	l := &logger{
		data:      make(map[string]map[string]interface{}),
		eventChan: make(chan *Event),
		done:      make(chan struct{}),
	}
	return l
}

func Run(config Config) {
	if !config.Enabled {
		return
	}
	go lg.run(config)
}

func (l *logger) run(config Config) {
	tick := time.NewTicker(config.Period)
	defer tick.Stop()
	for {
		select {
		case <-l.done:
			return

		case e := <-l.eventChan:
			d := make(map[string]interface{})
			err := json.Unmarshal(e.Data, &d)
			if err != nil {
				log.Warn("json marshal e.Data error: %v", err)
			}

			l.data[e.Topic] = d

		case <-tick.C:
			l.print()
			l.clean()
		}
	}
}

func (l *logger) print() {
	d, err := json.Marshal(l.data)
	if err != nil {
		log.Info("json marshal metric data err: %+v", err)
	}
	log.Info("[metric]: %s", d)
}

func (l *logger) stop() {
	close(l.done)
}

func (l *logger) clean() {
	for k := range l.data {
		delete(l.data, k)
	}
}

func Export(topic string, data []byte) {
	e := &Event{
		Topic: topic,
		Data:  data,
	}
	lg.eventChan <- e
}
