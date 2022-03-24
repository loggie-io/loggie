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

package filewatcher

import (
	"encoding/json"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/eventbus/export/logger"
	promeExporter "github.com/loggie-io/loggie/pkg/eventbus/export/prometheus"
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
	"time"
)

const name = "filewatcher"

func init() {
	eventbus.Registry(name, makeListener, eventbus.WithTopics([]string{eventbus.FileWatcherTopic}))
}

func makeListener() eventbus.Listener {
	l := &Listener{
		done:      make(chan struct{}),
		data:      make(map[string]data),
		config:    &Config{},
		eventChan: make(chan eventbus.WatchMetricData),
	}
	return l
}

type Config struct {
	Period            time.Duration `yaml:"period" default:"5m"`
	UnFinishedTimeout time.Duration `yaml:"checkUnFinishedTimeout" default:"24h"`
	FieldsRef         []string      `yaml:"fieldsRef"`
}

type Listener struct {
	config    *Config
	done      chan struct{}
	eventChan chan eventbus.WatchMetricData
	data      map[string]data // key=pipelineName+sourceName
}

type data struct {
	PipelineName string `json:"pipeline"`
	SourceName   string `json:"source"`

	FileInfo []*fileInfo `json:"info,omitempty"` // key=fileName

	TotalFileCount  int                    `json:"total"`
	InactiveFdCount int                    `json:"inactive"`
	SourceFields    map[string]interface{} `json:"sourceFields,omitempty"`
}

type fileInfo struct {
	FileName       string    `json:"name"`
	FileSize       int64     `json:"size"` // It will not be brought when reporting. It is obtained by directly using OS. Stat (filename). Size() on the consumer side
	AckOffset      int64     `json:"ackOffset"`
	LastModifyTime time.Time `json:"modify"`
	IgnoreOlder    bool      `json:"ignoreOlder"`
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
	e, ok := event.Data.(eventbus.WatchMetricData)
	if !ok {
		log.Panic("type assert eventbus.WatchMetricData failed: %v", ok)
	}
	l.eventChan <- e
}

func (l *Listener) exportPrometheus() {
	m := promeExporter.ExportedMetrics{}
	const FileNameKey = "filename"
	const FileStatusKey = "status"
	for _, d := range l.data {
		labels := prometheus.Labels{promeExporter.PipelineNameKey: d.PipelineName, promeExporter.SourceNameKey: d.SourceName}
		eventbus.InjectFields(labels, d.SourceFields)

		m1 := promeExporter.ExportedMetrics{
			{
				Desc: prometheus.NewDesc(
					buildFQName("total_file_count"),
					"file count total",
					nil, labels,
				),
				Eval:    float64(d.TotalFileCount),
				ValType: prometheus.GaugeValue,
			},
			{
				Desc: prometheus.NewDesc(
					buildFQName("inactive_file_count"),
					"inactive file count",
					nil, labels,
				),
				Eval:    float64(d.InactiveFdCount),
				ValType: prometheus.GaugeValue,
			},
		}
		for _, info := range d.FileInfo {
			status := "pending"
			if time.Since(info.LastModifyTime) > l.config.UnFinishedTimeout && util.Abs(info.FileSize-info.AckOffset) >= 1 {
				status = "unfinished"
			}
			if info.IgnoreOlder {
				status = "ignored"
			}
			labels[FileNameKey] = info.FileName
			labels[FileStatusKey] = status

			m2 := promeExporter.ExportedMetrics{
				{
					Desc: prometheus.NewDesc(
						buildFQName("file_size"),
						"file size",
						nil, labels,
					),
					Eval:    float64(info.FileSize),
					ValType: prometheus.GaugeValue,
				},
				{
					Desc: prometheus.NewDesc(
						buildFQName("file_ack_offset"),
						"file ack offset",
						nil, labels,
					),
					Eval:    float64(info.AckOffset),
					ValType: prometheus.GaugeValue,
				},
				{
					Desc: prometheus.NewDesc(
						buildFQName("file_last_modify"),
						"file last modify timestamp",
						nil, labels,
					),
					Eval:    float64(info.LastModifyTime.UnixNano() / 1e6),
					ValType: prometheus.GaugeValue,
				},
			}

			m1 = append(m1, m2...)
		}

		m = append(m, m1...)
	}
	promeExporter.Export(eventbus.FileWatcherTopic, m)
}

func buildFQName(name string) string {
	return prometheus.BuildFQName(promeExporter.Loggie, eventbus.FileWatcherTopic, name)
}

func (l *Listener) clean() {
	for k := range l.data {
		delete(l.data, k)
	}
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
			logger.Export(eventbus.FileWatcherTopic, m)

			l.clean()
		}
	}
}

func (l *Listener) consumer(e eventbus.WatchMetricData) {
	var buf strings.Builder
	buf.WriteString(e.PipelineName)
	buf.WriteString("-")
	buf.WriteString(e.SourceName)
	key := buf.String()

	m := data{
		PipelineName: e.PipelineName,
		SourceName:   e.SourceName,
		SourceFields: eventbus.GetFieldsByRef(l.config.FieldsRef, e.SourceFields),
	}

	var files []*fileInfo
	for _, fi := range e.FileInfos {
		f := &fileInfo{
			FileName:       fi.FileName,
			FileSize:       fi.Size,
			AckOffset:      fi.Offset,
			LastModifyTime: fi.LastModifyTime,
			IgnoreOlder:    fi.IsIgnoreOlder,
		}
		files = append(files, f)
	}
	m.FileInfo = files
	m.TotalFileCount = e.TotalFileCount
	m.InactiveFdCount = e.InactiveFdCount

	l.data[key] = m
}
