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

package normalize

import (
	"encoding/json"
	"fmt"
	"github.com/loggie-io/loggie/pkg/eventbus/export/logger"
	promeExporter "github.com/loggie-io/loggie/pkg/eventbus/export/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
)

const name = "normalize"

func init() {
	eventbus.Registry(name, makeListener, eventbus.WithTopics([]string{eventbus.NormalizeTopic, eventbus.ComponentBaseTopic}))
}

func makeListener() eventbus.Listener {
	l := &Listener{
		done:                 make(chan struct{}),
		config:               &Config{},
		eventChan:            make(chan eventbus.Event),
		normalizeMetric:      make(map[string]map[string]*eventbus.NormalizeMetricData),
		nameComponentMetrics: make(map[string][]eventbus.ComponentBaseMetricData),
	}
	return l
}

type Config struct {
	Period time.Duration `yaml:"period" default:"10s"`
}

type Listener struct {
	config               *Config
	done                 chan struct{}
	eventChan            chan eventbus.Event
	nameComponentMetrics map[string][]eventbus.ComponentBaseMetricData
	normalizeMetric      map[string]map[string]*eventbus.NormalizeMetricData // key=pipelineName+sourceName
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

func (l *Listener) Subscribe(event eventbus.Event) {
	l.eventChan <- event
}

func (l *Listener) Config() interface{} {
	return l.config
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
		}
	}
}

func (l *Listener) consumer(event eventbus.Event) {
	if event.Topic != eventbus.NormalizeTopic {
		return
	}

	normalizeEvent, ok := event.Data.(*eventbus.NormalizeMetricEvent)

	if ok == false {
		log.Error("convert eventbus failed: %v", normalizeEvent)
		return
	}

	key := fmt.Sprintf("%s:%s", normalizeEvent.PipelineName, normalizeEvent.Name)

	data, ok := l.normalizeMetric[key]

	if data == nil {
		l.normalizeMetric[key] = make(map[string]*eventbus.NormalizeMetricData)
	}

	if normalizeEvent.IsClear == true {

		if ok == false {
			return
		}

		delete(l.normalizeMetric, key)
		return
	}

	for _, metric := range normalizeEvent.MetricMap {
		value, ok := data[metric.Name]
		if !ok {
			l.normalizeMetric[key][metric.Name] = &eventbus.NormalizeMetricData{
				BaseInterceptorMetric: eventbus.BaseInterceptorMetric{
					PipelineName:    metric.BaseInterceptorMetric.PipelineName,
					InterceptorName: metric.BaseInterceptorMetric.InterceptorName,
				},
				Name:  metric.Name,
				Count: metric.Count,
			}
			continue
		}

		value.Count += metric.Count
	}
}

func buildFQName(name string) string {
	return prometheus.BuildFQName(promeExporter.Loggie, eventbus.NormalizeTopic, name)
}

func (l *Listener) exportPrometheus() {

	if len(l.normalizeMetric) == 0 {
		return
	}

	m := promeExporter.ExportedMetrics{}

	for _, d := range l.normalizeMetric {
		if len(d) == 0 {
			continue
		}

		for _, value := range d {
			labels := prometheus.Labels{
				promeExporter.PipelineNameKey:    value.PipelineName,
				promeExporter.InterceptorNameKey: value.Name,
				"name":                           value.InterceptorName,
			}

			m1 := promeExporter.ExportedMetrics{
				{
					Desc: prometheus.NewDesc(
						buildFQName("error_count"),
						"error count",
						nil, labels,
					),
					Eval:    float64(value.Count),
					ValType: prometheus.CounterValue,
				},
			}

			m = append(m, m1...)
		}
	}

	promeExporter.Export(eventbus.NormalizeTopic, m)
	logData, _ := json.Marshal(l.normalizeMetric)
	logger.Export(eventbus.NormalizeTopic, logData)
}
