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

package filestream

import (
	"encoding/json"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/eventbus/export/logger"
	promeExporter "github.com/loggie-io/loggie/pkg/eventbus/export/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
	"time"
)

const name = "filestream"

func init() {
	eventbus.Registry(name, makeListener, eventbus.WithTopics([]string{eventbus.FileStreamMetricTopic}))
}

func makeListener() eventbus.Listener {
	l := &Listener{
		done:      make(chan struct{}),
		data:      make(map[string]data),
		config:    &Config{},
		eventChan: make(chan eventbus.FileStreamCollectMetricData),
	}
	return l
}

type Config struct {
	Period    time.Duration `yaml:"period" default:"10s"`
	FieldsRef []string      `yaml:"fieldsRef"`
}

type Listener struct {
	config    *Config
	done      chan struct{}
	eventChan chan eventbus.FileStreamCollectMetricData
	data      map[string]data // key=pipelineName+sourceName
}

type data struct {
	PipelineName  string
	SourceName    string
	SourceFields  map[string]interface{}
	FileHarvester map[string]*streamFileHarvester
}

type streamFileHarvester struct {
	FileCount        int
	ProductEvent     uint64
	DurationTime     uint64
	ScanDurationTime uint64
	WaitAckNumber    int
}

func (l *Listener) Name() string {
	return name
}

func (l *Listener) Init(ctx api.Context) error {
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
	e, ok := event.Data.(eventbus.FileStreamCollectMetricData)
	if !ok {
		log.Panic("type assert eventbus.FileStreamCollectMetricData failed: %v", ok)
	}
	l.eventChan <- e
}

func (l *Listener) exportPrometheus() {
	m := promeExporter.ExportedMetrics{}

	if len(l.data) == 0 {
		return
	}

	const StreamNameKey = "stream"
	for _, d := range l.data {
		labels := prometheus.Labels{
			promeExporter.PipelineNameKey: d.PipelineName,
			promeExporter.SourceNameKey:   d.SourceName,
		}
		eventbus.InjectFields(labels, d.SourceFields)

		if len(d.FileHarvester) == 0 {
			return
		}

		for streamName, harvester := range d.FileHarvester {
			labels[StreamNameKey] = streamName
			m1 := promeExporter.ExportedMetrics{
				{
					Desc: prometheus.NewDesc(
						buildFQName("stream_file_count"),
						"file size",
						nil, labels,
					),
					Eval:    float64(harvester.FileCount),
					ValType: prometheus.GaugeValue,
				},
				{
					Desc: prometheus.NewDesc(
						buildFQName("batch_send_duration"),
						"handle stream duration",
						nil, labels,
					),
					Eval:    float64(harvester.DurationTime),
					ValType: prometheus.GaugeValue,
				},
				{
					Desc: prometheus.NewDesc(
						buildFQName("scan_files_duration"),
						"check stream duration",
						nil, labels,
					),
					Eval:    float64(harvester.ScanDurationTime),
					ValType: prometheus.GaugeValue,
				},

				{
					Desc: prometheus.NewDesc(
						buildFQName("stream_product_counter"),
						"handle stream duration",
						nil, labels,
					),
					Eval:    float64(harvester.ProductEvent),
					ValType: prometheus.CounterValue,
				},
				{
					Desc: prometheus.NewDesc(
						buildFQName("stream_wait_ack_number"),
						"handle stream duration",
						nil, labels,
					),
					Eval:    float64(harvester.WaitAckNumber),
					ValType: prometheus.CounterValue,
				},
			}

			if harvester.DurationTime > 0 {
				harvester.DurationTime = 0
			}
			if harvester.ScanDurationTime > 0 {
				harvester.ScanDurationTime = 0
			}

			m = append(m, m1...)
		}

	}

	if len(m) == 0 {
		return
	}
	promeExporter.Export(eventbus.FileStreamMetricTopic, m)
}

func buildFQName(name string) string {
	return prometheus.BuildFQName(promeExporter.Loggie, eventbus.FileStreamMetricTopic, name)
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
			logger.Export(eventbus.FileStreamMetricTopic, m)
		}
	}
}

func (l *Listener) consumer(e eventbus.FileStreamCollectMetricData) {
	var buf strings.Builder
	buf.WriteString(e.PipelineName)
	buf.WriteString("-")
	buf.WriteString(e.SourceName)
	key := buf.String()

	metric, ok := l.data[key]

	if e.Done == true {
		if ok {
			delete(l.data, key)
			log.Info("%s metric delete", key)
		}
		return
	}

	if e.StreamName == "" {
		return
	}

	if !ok {
		m := data{
			PipelineName: e.PipelineName,
			SourceName:   e.SourceName,
			SourceFields: eventbus.GetFieldsByRef(l.config.FieldsRef, e.SourceFields),
		}

		h := map[string]*streamFileHarvester{
			e.StreamName: {
				FileCount:        e.FilesCount,
				ProductEvent:     e.ProductCount,
				DurationTime:     e.DurationTime,
				ScanDurationTime: e.ScanDurationTime,
				WaitAckNumber:    e.ChangeAckNumber,
			},
		}
		m.FileHarvester = h
		l.data[key] = m
		return
	}

	if metric.FileHarvester == nil {
		metric.FileHarvester = map[string]*streamFileHarvester{}
	}

	harvester, ok := metric.FileHarvester[e.StreamName]
	if !ok {
		h := streamFileHarvester{
			FileCount:        e.FilesCount,
			ProductEvent:     e.ProductCount,
			DurationTime:     e.DurationTime,
			ScanDurationTime: e.ScanDurationTime,
			WaitAckNumber:    e.ChangeAckNumber,
		}
		metric.FileHarvester[e.StreamName] = &h
		return
	}
	harvester.FileCount = e.FilesCount
	harvester.ProductEvent += e.ProductCount
	if e.DurationTime > 0 {
		harvester.DurationTime = e.DurationTime
	}

	if e.ScanDurationTime > 0 {
		harvester.ScanDurationTime = e.ScanDurationTime
	}
	harvester.WaitAckNumber = e.ChangeAckNumber

	if e.ClearAckNumber == true {
		harvester.WaitAckNumber = 0
	}
	return
}
