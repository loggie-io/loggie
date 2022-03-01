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

package reload

import (
	"encoding/json"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/eventbus/export/logger"
	promeExporter "github.com/loggie-io/loggie/pkg/eventbus/export/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

const name = "reload"

func init() {
	eventbus.Registry(name, makeListener, eventbus.WithTopic(eventbus.ReloadTopic))
}

func makeListener() eventbus.Listener {
	l := &Listener{
		done: make(chan struct{}),
		data: data{
			ReloadTotal: 0,
		},
		config: &Config{},
	}
	return l
}

type Config struct {
	Period time.Duration `yaml:"period" default:"10s"`
}

type Listener struct {
	config *Config
	data   data
	done   chan struct{}
}

type data struct {
	ReloadTotal float64 `yaml:"total"`
}

func (l *Listener) Name() string {
	return name
}

func (l *Listener) Init(ctx api.Context) {
}

func (l *Listener) Start() {
	go l.export()
}

func (l *Listener) Stop() {
	close(l.done)
}

func (l *Listener) Subscribe(event eventbus.Event) {
	d, ok := event.Data.(eventbus.ReloadMetricData)
	if !ok {
		log.Panic("convert eventbus reload failed: %v", ok)
	}

	l.data.ReloadTotal = l.data.ReloadTotal + float64(d.Tick)
}

func (l *Listener) Config() interface{} {
	return l.config
}

func (l *Listener) export() {
	tick := time.NewTicker(l.config.Period)
	defer tick.Stop()
	for {
		select {
		case <-l.done:
			return
		case <-tick.C:
			l.exportPrometheus()
			m, _ := json.Marshal(l.data)
			logger.Export(eventbus.ReloadTopic, m)
		}
	}
}
func (l *Listener) exportPrometheus() {
	metric := promeExporter.ExportedMetrics{
		{
			Desc: prometheus.NewDesc(
				prometheus.BuildFQName(promeExporter.Loggie, eventbus.ReloadTopic, "total"),
				"Loggie reload total count",
				nil, nil,
			),
			Eval:    l.data.ReloadTotal,
			ValType: prometheus.CounterValue,
		},
	}
	promeExporter.Export(eventbus.ReloadTopic, metric)
}
