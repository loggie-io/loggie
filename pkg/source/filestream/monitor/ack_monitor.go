package monitor

import (
	"github.com/loggie-io/loggie/pkg/eventbus"
)

type AckMonitor struct {
	export *MetricReporter
	fields map[string]interface{}
}

func NewAckMonitor(export *MetricReporter, fields map[string]interface{}) *AckMonitor {
	return &AckMonitor{
		export: export,
		fields: fields,
	}
}

func (s *AckMonitor) AckCollect(streamName string, count int) {
	var context eventbus.FileStreamCollectMetricData
	context.DurationTime = 0
	context.ChangeAckNumber = count
	context.StreamName = streamName
	context.SourceFields = s.fields
	s.export.reportMetric(&context)
}
