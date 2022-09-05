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

package info

import (
	"github.com/loggie-io/loggie/pkg/core/global"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/eventbus"
	promeExporter "github.com/loggie-io/loggie/pkg/eventbus/export/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

const name = "info"

func init() {
	eventbus.Registry(name, makeListener, eventbus.WithTopic(eventbus.InfoTopic))
}

func makeListener() eventbus.Listener {
	l := &Listener{
		done:   make(chan struct{}),
		config: &Config{},
	}
	return l
}

type Config struct {
	Period time.Duration `yaml:"period" default:"10s"`
}

type Listener struct {
	config *Config
	done   chan struct{}
}

func (l *Listener) Name() string {
	return name
}

func (l *Listener) Init(ctx api.Context) error {
	return nil
}

func (l *Listener) Start() error {
	go l.export()
	return nil
}

func (l *Listener) Stop() {
	close(l.done)
}

func (l *Listener) Subscribe(event eventbus.Event) {
	// Do nothing
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
		}
	}
}

func (l *Listener) exportPrometheus() {
	labels := prometheus.Labels{
		"version": global.GetVersion(),
	}
	metric := promeExporter.ExportedMetrics{
		{
			Desc: prometheus.NewDesc(
				prometheus.BuildFQName(promeExporter.Loggie, eventbus.InfoTopic, "status"),
				"Loggie info",
				nil, labels,
			),
			Eval:    float64(1),
			ValType: prometheus.GaugeValue,
		},
	}
	promeExporter.Export(eventbus.InfoTopic, metric)
}
