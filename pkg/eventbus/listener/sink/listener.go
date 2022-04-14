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

package sink

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/eventbus/export/logger"
	promeExporter "github.com/loggie-io/loggie/pkg/eventbus/export/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

const name = "sink"

func init() {
	eventbus.Registry(name, makeListener, eventbus.WithTopic(eventbus.SinkMetricTopic))
}

func makeListener() eventbus.Listener {
	l := &Listener{
		data:      make(map[string]*data),
		done:      make(chan struct{}),
		config:    &Config{},
		eventChan: make(chan eventbus.SinkMetricData),
	}
	return l
}

type Config struct {
	Period time.Duration `yaml:"period" default:"10s"`
}

type Listener struct {
	config    *Config
	data      map[string]*data // key=pipelineName+sourceName
	eventChan chan eventbus.SinkMetricData
	done      chan struct{}
}

type data struct {
	PipelineName string `json:"pipeline"`
	SourceName   string `json:"source"`

	SuccessEventCount int64 `json:"successEvent"`
	FailEventCount    int64 `json:"failedEvent"`

	EventQps float64 `json:"eventQps"`
}

func (l *Listener) Name() string {
	return name
}

func (l *Listener) Init(context api.Context) error {
	return nil
}

func (l *Listener) Start() error {
	go l.run()
	return nil
}

func (l *Listener) Stop() {
	close(l.done)
}

func (l *Listener) Config() interface{} {
	return l.config
}

func (l *Listener) Subscribe(event eventbus.Event) {
	e, ok := event.Data.(eventbus.SinkMetricData)
	if !ok {
		log.Panic("type assert eventbus.SinkMetricData failed: %v", ok)
	}
	l.eventChan <- e
}

func (l *Listener) run() {
	tick := time.NewTicker(l.config.Period)
	defer tick.Stop()
	for {
		select {
		case <-l.done:
			return

		case e := <-l.eventChan:
			l.consumer(e)

		case <-tick.C:
			l.compute()
			l.exportPrometheus()
			m, _ := json.Marshal(l.data)
			logger.Export(eventbus.SinkMetricTopic, m)
			l.clean()
		}
	}
}

func (l *Listener) exportPrometheus() {

	metrics := promeExporter.ExportedMetrics{}
	for _, d := range l.data {
		m := promeExporter.ExportedMetrics{
			{
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(promeExporter.Loggie, eventbus.SinkMetricTopic, "success_event"),
					"send event success count",
					nil, prometheus.Labels{promeExporter.PipelineNameKey: d.PipelineName, promeExporter.SourceNameKey: d.SourceName},
				),
				Eval:    float64(d.SuccessEventCount),
				ValType: prometheus.GaugeValue,
			},
			{
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(promeExporter.Loggie, eventbus.SinkMetricTopic, "failed_event"),
					"send event failed count",
					nil, prometheus.Labels{promeExporter.PipelineNameKey: d.PipelineName, promeExporter.SourceNameKey: d.SourceName},
				),
				Eval:    float64(d.FailEventCount),
				ValType: prometheus.GaugeValue,
			},
			{
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(promeExporter.Loggie, eventbus.SinkMetricTopic, "event_qps"),
					"send success event failed count",
					nil, prometheus.Labels{promeExporter.PipelineNameKey: d.PipelineName, promeExporter.SourceNameKey: d.SourceName},
				),
				Eval:    d.EventQps,
				ValType: prometheus.GaugeValue,
			},
		}

		metrics = append(metrics, m...)
	}
	promeExporter.Export(eventbus.SinkMetricTopic, metrics)
}

func (l *Listener) compute() {
	for _, d := range l.data {
		d.EventQps = float64(d.SuccessEventCount) / l.config.Period.Seconds()
	}
}

func (l *Listener) clean() {
	for k := range l.data {
		delete(l.data, k)
	}
}

func (l *Listener) consumer(e eventbus.SinkMetricData) {
	var buf strings.Builder
	buf.WriteString(e.PipelineName)
	buf.WriteString("-")
	buf.WriteString(e.SourceName)
	key := buf.String()

	d, ok := l.data[key]
	if !ok {
		data := &data{
			PipelineName: e.PipelineName,
			SourceName:   e.SourceName,
		}
		if e.FailEventCount != 0 {
			data.FailEventCount = int64(e.FailEventCount)
		}
		if e.SuccessEventCount != 0 {
			data.SuccessEventCount = int64(e.SuccessEventCount)
		}
		l.data[key] = data
		return
	}

	if e.FailEventCount != 0 {
		d.FailEventCount += int64(e.FailEventCount)
	}
	if e.SuccessEventCount != 0 {
		d.SuccessEventCount += int64(e.SuccessEventCount)
	}
}
