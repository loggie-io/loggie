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
	Enabled bool          `yaml:"enabled,omitempty"`
	Period  time.Duration `yaml:"period,omitempty" default:"10s"`
	Pretty bool `yaml:"pretty,omitempty"`
	AdditionLogEnabled bool `yaml:"additionLogEnabled,omitempty"`
	AdditionLogConfig log.LoggerConfig `yaml:"additionLogConfig,omitempty"`
}

func (c *Config) SetDefaults() {
	if c.AdditionLogEnabled {
		config := c.AdditionLogConfig

		config.EnableStdout = false
		config.EnableFile = true
		config.Level = "info"
		if config.Filename == "" {
			config.Filename = "metrics.log"
		}
		if config.Directory == "" {
			config.Directory = "/data/loggie/log"
		}
		if config.MaxBackups == 0 {
			config.MaxBackups = 3
		}
		if config.MaxAge == 0 {
			config.MaxAge = 14
		}
		if config.MaxSize == 0 {
			config.MaxSize = 1024
		}
		if config.TimeFormat == "" {
			config.TimeFormat = "2006-01-02 15:04:05"
		}

		c.AdditionLogConfig = config
	}
}

type logger struct {
	data map[string]map[string]interface{} // key=topic

	eventChan chan *Event

	done chan struct{}
	config Config
	additionLogger *log.Logger
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
	l.config = config

	if l.config.AdditionLogEnabled {
		l.additionLogger = log.NewLogger(&l.config.AdditionLogConfig)
	}

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

	var d []byte
	var err error
	if l.config.Pretty {
		d, err = json.MarshalIndent(l.data, "", "  ")
	} else {
		d, err = json.Marshal(l.data)
	}
	if err != nil {
		log.Warn("json marshal metric data err: %+v", err)
	}

	if !l.config.AdditionLogEnabled {
		log.Info("[metric]: %s", d)
	} else {
		l.additionLogger.RawJson("metrics", d, "")
	}
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
