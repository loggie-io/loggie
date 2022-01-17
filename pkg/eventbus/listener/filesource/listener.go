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

package filesource

import (
	"encoding/json"
	"github.com/prometheus/client_golang/prometheus"
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/eventbus"
	"loggie.io/loggie/pkg/eventbus/export/logger"
	promeExporter "loggie.io/loggie/pkg/eventbus/export/prometheus"
	"os"
	"strings"
	"time"
)

func init() {
	eventbus.Registry(makeListener(), eventbus.WithTopics([]string{eventbus.FileSourceMetricTopic}))
}

func makeListener() *Listener {
	l := &Listener{
		eventChan: make(chan eventbus.CollectMetricData),
		done:      make(chan struct{}),
		data:      make(map[string]data),
		config:    &Config{},
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
	eventChan chan eventbus.CollectMetricData
	data      map[string]data // key=pipelineName+sourceName
}

type data struct {
	PipelineName string                 `json:"pipeline"`
	SourceName   string                 `json:"source"`
	SourceFields map[string]interface{} `json:"sourceFields,omitempty"`

	FileHarvester map[string]*fileHarvester `json:"harvester,omitempty"` // key=fileName
}

type fileHarvester struct {
	FileName   string  `json:"fileName"`
	Offset     int64   `json:"offset"`
	LineNumber int64   `json:"currentLine"`
	TotalLine  int64   `json:"readLines"`
	LineQps    float64 `json:"lineQps"`
	FileSize   int64   `json:"fileSize"` // It will not be brought when reporting. It is obtained by directly using OS. Stat (filename). Size() on the consumer side
}

func (l *Listener) Name() string {
	return "filesource"
}

func (l *Listener) Init(ctx api.Context) {
}

func (l *Listener) Start() {
	go l.run()
}

func (l *Listener) Stop() {
	close(l.done)
}

func (l *Listener) Config() interface{} {
	return l.config
}

func (l *Listener) Subscribe(event eventbus.Event) {
	e, ok := event.Data.(eventbus.CollectMetricData)
	if !ok {
		log.Panic("type assert eventbus.CollectMetricData failed: %v", ok)
	}
	l.eventChan <- e
}

func (l *Listener) exportPrometheus() {
	m := promeExporter.ExportedMetrics{}
	const FileNameKey = "filename"
	for _, d := range l.data {
		labels := prometheus.Labels{promeExporter.PipelineNameKey: d.PipelineName, promeExporter.SourceNameKey: d.SourceName}
		eventbus.InjectFields(labels, d.SourceFields)

		for _, harvester := range d.FileHarvester {
			labels[FileNameKey] = harvester.FileName
			m1 := promeExporter.ExportedMetrics{
				{
					Desc: prometheus.NewDesc(
						buildFQName("file_size"),
						"file size",
						nil, labels,
					),
					Eval:    float64(harvester.FileSize),
					ValType: prometheus.GaugeValue,
				},
				{
					Desc: prometheus.NewDesc(
						buildFQName("file_offset"),
						"file offset",
						nil, labels,
					),
					Eval:    float64(harvester.Offset),
					ValType: prometheus.GaugeValue,
				},
				{
					Desc: prometheus.NewDesc(
						buildFQName("line_number"),
						"current read line number",
						nil, labels,
					),
					Eval:    float64(harvester.LineNumber),
					ValType: prometheus.GaugeValue,
				},
				{
					Desc: prometheus.NewDesc(
						buildFQName("line_qps"),
						"current read line qps",
						nil, labels,
					),
					Eval:    harvester.LineQps,
					ValType: prometheus.GaugeValue,
				},
			}

			m = append(m, m1...)
		}

	}
	promeExporter.Export(eventbus.FileSourceMetricTopic, m)
}

func buildFQName(name string) string {
	return prometheus.BuildFQName(promeExporter.Loggie, eventbus.FileSourceMetricTopic, name)
}

func (l *Listener) compute() {
	for _, d := range l.data {
		// set file size
		for k, h := range d.FileHarvester {
			f, err := os.Stat(h.FileName)
			if err != nil {
				log.Warn("stat file %s error: %v", h.FileName, err)
				continue
			}

			d.FileHarvester[k].FileSize = f.Size()
			d.FileHarvester[k].LineQps = float64(h.TotalLine) / l.config.Period.Seconds()
		}
	}
}

func (l *Listener) clean() {
	for k := range l.data {
		delete(l.data, k)
	}
}

func (l *Listener) run() {
	tick := time.Tick(l.config.Period)
	for {
		select {
		case <-l.done:
			return

		case e := <-l.eventChan:
			l.consumer(e)

		case <-tick:
			l.compute()
			l.exportPrometheus()

			m, _ := json.Marshal(l.data)
			logger.Export(eventbus.FileSourceMetricTopic, m)

			l.clean()
		}
	}
}

func (l *Listener) consumer(e eventbus.CollectMetricData) {
	var buf strings.Builder
	buf.WriteString(e.PipelineName)
	buf.WriteString("-")
	buf.WriteString(e.SourceName)
	key := buf.String()

	metric, ok := l.data[key]
	// if pipeline metrics not exist
	if !ok {
		m := data{
			PipelineName: e.PipelineName,
			SourceName:   e.SourceName,
			SourceFields: eventbus.GetFieldsByRef(l.config.FieldsRef, e.SourceFields),
		}

		h := map[string]*fileHarvester{
			e.FileName: {
				FileName:   e.FileName,
				Offset:     e.Offset,
				LineNumber: e.LineNumber,
				TotalLine:  e.Lines,
				FileSize:   e.FileSize,
			},
		}
		m.FileHarvester = h
		l.data[key] = m
		return
	}

	if metric.FileHarvester == nil {
		metric.FileHarvester = map[string]*fileHarvester{}
	}
	harvester, ok := metric.FileHarvester[e.FileName]
	if !ok {
		h := fileHarvester{
			FileName:   e.FileName,
			Offset:     e.Offset,
			LineNumber: e.LineNumber,
			TotalLine:  e.Lines,
			FileSize:   e.FileSize,
		}
		metric.FileHarvester[e.FileName] = &h
		return
	}

	harvester.FileName = e.FileName
	if e.Offset != 0 {
		harvester.Offset = e.Offset
	}
	if e.LineNumber != 0 {
		harvester.LineNumber = e.LineNumber
	}
	harvester.TotalLine += e.Lines
}
