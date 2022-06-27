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

package prometheus

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	Loggie             = "loggie"
	PipelineNameKey    = "pipeline"
	SourceNameKey      = "source"
	InterceptorNameKey = "interceptor"
	QueueTypeKey       = "type"
)

var collector *Collector

func init() {
	http.Handle("/metrics", HandlePromMetrics())
	collector = NewCollector()
	prometheus.MustRegister(collector)
}

// HandlePromMetrics export prometheus metrics
func HandlePromMetrics() http.Handler {
	return promhttp.Handler()
}

type Collector struct {
	Metrics sync.Map

	mutex sync.Mutex
}

func NewCollector() *Collector {
	return &Collector{
		Metrics: sync.Map{},
	}
}

type ExportedMetrics []struct {
	Desc    *prometheus.Desc
	Eval    float64
	ValType prometheus.ValueType
}

func Export(topic string, m ExportedMetrics) {
	collector.Metrics.Store(topic, m)
}

// Describe returns all descriptions of the collector.
func (b *Collector) Describe(ch chan<- *prometheus.Desc) {
	b.Metrics.Range(func(key, value interface{}) bool {
		m := value.(ExportedMetrics)
		for _, metric := range m {
			ch <- metric.Desc
		}
		return true
	})
}

func (b *Collector) Collect(ch chan<- prometheus.Metric) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.Metrics.Range(func(key, value interface{}) bool {
		m := value.(ExportedMetrics)
		for _, i := range m {
			ch <- prometheus.MustNewConstMetric(i.Desc, i.ValType, i.Eval)
		}
		return true
	})
}
