package monitor

import (
	"github.com/loggie-io/loggie/pkg/eventbus"
	"time"
)

type Monitor struct {
	streamName string
	fields     map[string]interface{}
	exporter   *MetricReporter
}

func NewMonitor(exporter *MetricReporter, streamName string, fields map[string]interface{}) *Monitor {
	return &Monitor{
		streamName: streamName,
		fields:     fields,
		exporter:   exporter,
	}
}

func (s *Monitor) BeginTime() int64 {
	return time.Now().UnixNano()
}

// CountScan 统计文件列表的扫描情况
func (s *Monitor) CountScan(begin int64, fileLen int) {
	var context eventbus.FileStreamCollectMetricData
	duration := time.Now().UnixNano() - begin
	context.ScanDurationTime = uint64(duration)
	context.FilesCount = fileLen
	context.StreamName = s.streamName
	context.SourceFields = s.fields
	s.exporter.reportMetric(&context)
}

// CountCollect 统计文件收集
func (s *Monitor) CountCollect(begin int64, count uint64) {
	var context eventbus.FileStreamCollectMetricData
	duration := time.Now().UnixNano() - begin
	context.DurationTime = uint64(duration)
	context.ProductCount = count
	context.StreamName = s.streamName
	context.SourceFields = s.fields
	s.exporter.reportMetric(&context)
}
