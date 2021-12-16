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

package queue

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/eventbus"
	"loggie.io/loggie/pkg/eventbus/export/logger"
	promeExporter "loggie.io/loggie/pkg/eventbus/export/prometheus"
)

func init() {
	eventbus.Registry(makeListener(), eventbus.WithTopic(eventbus.QueueMetricTopic))
}

func makeListener() *Listener {
	l := &Listener{
		data:   make(map[string]*data),
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
	data   map[string]*data // key=pipelineName+type
	done   chan struct{}
}

type data struct {
	PipelineName string `json:"pipeline"`
	QueueType    string `json:"queueType"`

	Capacity       int64   `json:"capacity"`
	Size           int64   `json:"size"`
	FillPercentage float64 `json:"fillPercentage"`
}

func (l *Listener) Name() string {
	return "queue"
}

func (l *Listener) Init(ctx api.Context) {
}

func (l *Listener) Start() {
	go l.export()
}

func (l *Listener) Stop() {
	close(l.done)
}

func (l *Listener) Config() interface{} {
	return l.config
}

func (l *Listener) Subscribe(event eventbus.Event) {
	e, ok := event.Data.(eventbus.QueueMetricData)
	if !ok {
		log.Panic("convert eventbus queue failed: %v", ok)
	}
	var buf strings.Builder
	buf.WriteString(e.PipelineName)
	buf.WriteString("-")
	buf.WriteString(e.Type)
	key := buf.String()

	d, ok := l.data[key]
	if !ok {
		data := &data{
			PipelineName: e.PipelineName,
			QueueType:    e.Type,

			Size:     e.Size,
			Capacity: e.Capacity,
		}

		if e.Capacity > 0 {
			data.FillPercentage = float64(e.Size) / float64(e.Capacity)
		}

		l.data[key] = data
		return
	}

	d.Capacity = e.Capacity
	d.Size = e.Size
	if e.Capacity > 0 {
		d.FillPercentage = float64(e.Size) / float64(e.Capacity)
	}
}

func (l *Listener) export() {
	tick := time.Tick(l.config.Period)
	for {
		select {
		case <-l.done:
			return
		case <-tick:
			l.exportPrometheus()
			m, _ := json.Marshal(l.data)
			logger.Export(eventbus.QueueMetricTopic, m)
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
					prometheus.BuildFQName(promeExporter.Loggie, eventbus.QueueMetricTopic, "capacity"),
					"queue capacity",
					nil,
					prometheus.Labels{
						promeExporter.PipelineNameKey: d.PipelineName,
						promeExporter.QueueTypeKey:    d.QueueType,
					},
				),
				Eval:    float64(d.Capacity),
				ValType: prometheus.GaugeValue,
			},
			{
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(promeExporter.Loggie, eventbus.QueueMetricTopic, "size"),
					"queue size",
					nil,
					prometheus.Labels{
						promeExporter.PipelineNameKey: d.PipelineName,
						promeExporter.QueueTypeKey:    d.QueueType,
					},
				),
				Eval:    float64(d.Size),
				ValType: prometheus.GaugeValue,
			},
			{
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(promeExporter.Loggie, eventbus.QueueMetricTopic, "fill_percentage"),
					"how full is queue",
					nil,
					prometheus.Labels{
						promeExporter.PipelineNameKey: d.PipelineName,
						promeExporter.QueueTypeKey:    d.QueueType,
					},
				),
				Eval:    d.FillPercentage,
				ValType: prometheus.GaugeValue,
			},
		}

		metrics = append(metrics, m...)
	}
	promeExporter.Export(eventbus.SinkMetricTopic, metrics)
}

func (l *Listener) clean() {
	for k := range l.data {
		delete(l.data, k)
	}
}
