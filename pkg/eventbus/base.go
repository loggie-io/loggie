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

package eventbus

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
)

const (
	ComponentStop  = ComponentEventType("stop")
	ComponentStart = ComponentEventType("start")
)

var (
	FileSourceMetricTopic = "filesource"
	FileWatcherTopic      = "filewatcher"
	SinkMetricTopic       = "sink"
	ReloadTopic           = "reload"
	ErrorTopic            = "error"
	LogAlertTopic         = "log"
	QueueMetricTopic      = "queue"
	PipelineTopic         = "pipeline"
	ComponentBaseTopic    = "component"
	SystemTopic           = "sys"
	NormalizeTopic        = "normalize"
)

type BaseMetric struct {
	PipelineName string
	SourceName   string
}

type BaseInterceptorMetric struct {
	PipelineName    string
	InterceptorName string
}

type BaseMetricData struct {
	Lines int
	Bytes int
}

type CollectMetricData struct {
	BaseMetric
	FileName     string // including path
	Offset       int64
	LineNumber   int64 // file lines count
	Lines        int64 // current line offset
	FileSize     int64
	SourceFields map[string]interface{}
}

type SinkMetricData struct {
	BaseMetric
	SuccessEventCount int
	FailEventCount    int
}

type QueueMetricData struct {
	PipelineName string
	Type         string
	Capacity     int64
	Size         int64
}

type ReloadMetricData struct {
	Tick int
}

type ComponentBaseMetricData struct {
	EventType    ComponentEventType // "start","stop"...
	PipelineName string
	EpochTime    time.Time
	Config       ComponentBaseConfig
}

type ComponentEventType string

type ErrorMetricData struct {
	ErrorMsg string
}

type WatchMetricData struct {
	BaseMetric
	FileInfos       []FileInfo
	TotalFileCount  int
	InactiveFdCount int
	SourceFields    map[string]interface{}
}

type FileInfo struct {
	FileName       string
	Size           int64
	LastModifyTime time.Time
	Offset         int64
	IsIgnoreOlder  bool
	IsRelease      bool
}

type LogAlertData struct {
	Labels      map[string]string
	Annotations map[string]string
}

func NewLogAlertData(labels map[string]string, annotations map[string]string) *LogAlertData {
	return &LogAlertData{
		Labels:      labels,
		Annotations: annotations,
	}
}

func (lad *LogAlertData) Fingerprint() string {
	labelStr := fmt.Sprint(lad.Labels)
	out := md5.Sum([]byte(labelStr))
	return hex.EncodeToString(out[:])
}

type PipelineMetricData struct {
	EventType        ComponentEventType
	Name             string
	Time             time.Time
	ComponentConfigs []ComponentBaseConfig
}

type NormalizeMetricData struct {
	BaseInterceptorMetric
	Count uint64
	Name  string
}

type NormalizeMetricEvent struct {
	MetricMap    map[string]*NormalizeMetricData
	PipelineName string
	Name         string
	IsClear      bool
}

type ComponentBaseConfig struct {
	Name     string
	Type     api.Type
	Category api.Category
}

func (cbc ComponentBaseConfig) Code() string {
	var code strings.Builder
	code.WriteString(string(cbc.Type))
	code.WriteString(":")
	code.WriteString(cbc.Name)
	return code.String()
}

func GetFieldsByRef(fieldsRef []string, fields map[string]interface{}) map[string]interface{} {
	if fieldsRef == nil {
		return nil
	}

	res := make(map[string]interface{})
	for _, key := range fieldsRef {
		if val, ok := fields[key]; ok {
			res[key] = val
		}
	}
	return res
}

func InjectFields(labels map[string]string, fields map[string]interface{}) {
	if len(fields) > 0 {
		for k, v := range fields {
			vs, ok := v.(string)
			if !ok {
				log.Debug("fields %v is not string", v)
				continue
			}
			labels[k] = vs
		}
	}
}
