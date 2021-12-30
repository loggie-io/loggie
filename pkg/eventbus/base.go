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
	"loggie.io/loggie/pkg/core/api"
	"strings"
	"time"
)

const (
	ComponentStop  = ComponentEventType(0)
	ComponentStart = ComponentEventType(1)
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
)

type BaseMetric struct {
	PipelineName string
	SourceName   string
}

type BaseMetricData struct {
	Lines int
	Bytes int
}

type CollectMetricData struct {
	BaseMetric
	FileName   string // including path
	Offset     int64
	LineNumber int64 // file lines count
	Lines      int64 // current line offset
	FileSize   int64
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
	Type         ComponentEventType // "start","stop"...
	PipelineName string
	EpochTime    time.Time
	Config       ComponentBaseConfig
}

type ComponentEventType int32

type ErrorMetricData struct {
	ErrorMsg string
}

type WatchMetricData struct {
	BaseMetric
	FileInfos       []FileInfo
	TotalFileCount  int
	InactiveFdCount int
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
	Type             ComponentEventType
	Name             string
	Time             time.Time
	ComponentConfigs []ComponentBaseConfig
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
