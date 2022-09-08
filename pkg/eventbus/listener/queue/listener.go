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

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/eventbus/export/logger"
	promeExporter "github.com/loggie-io/loggie/pkg/eventbus/export/prometheus"
)

const name = "queue"

func init() {
	eventbus.Registry(name, makeListener, eventbus.WithTopic(eventbus.QueueMetricTopic))
}

func makeListener() eventbus.Listener {
	l := &Listener{
		eventChan: make(chan eventbus.QueueMetricData),
		data:      make(map[string]metricData),
		done:      make(chan struct{}),
		config:    &Config{},
	}
	return l
}

type Config struct {
	Period time.Duration `yaml:"period" default:"10s"`
}

type Listener struct {
	config    *Config
	eventChan chan eventbus.QueueMetricData
	data      map[string]metricData // key=pipelineName+type
	done      chan struct{}
}

type metricData struct {
	PipelineName string `json:"pipeline"`
	QueueType    string `json:"queueType"`

	Capacity       int64   `json:"capacity"`
	Size           int64   `json:"size"`
	FillPercentage float64 `json:"fillPercentage"`
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
	e, ok := event.Data.(eventbus.QueueMetricData)
	if !ok {
		log.Panic("type assert eventbus.QueueMetricData failed: %v", ok)
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
	promeExporter.Export(eventbus.QueueMetricTopic, metrics)
}

func (l *Listener) consumer(e eventbus.QueueMetricData) {
	var buf strings.Builder
	buf.WriteString(e.PipelineName)
	buf.WriteString("-")
	buf.WriteString(e.Type)
	key := buf.String()

	d, ok := l.data[key]
	if !ok {
		data := metricData{
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

func (l *Listener) clean() {
	for k := range l.data {
		delete(l.data, k)
	}
}
