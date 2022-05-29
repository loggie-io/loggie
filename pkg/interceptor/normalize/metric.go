package normalize

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
)

func (i *Interceptor) reportMetric(process Processor) {
	if process == nil {
		// file is not really being collected
		log.Error("process is nil")
		return
	}
	v, ok := i.metricMap[process.GetName()]
	if !ok {
		normalizeMetricData := eventbus.NormalizeMetricData{
			BaseMetric: eventbus.BaseMetric{
				PipelineName: i.pipelineName,
				SourceName:   i.name,
			},
			Name:  process.GetName(),
			Count: 1,
		}
		i.metricMap[process.GetName()] = &normalizeMetricData
		return
	}

	v.Count++
	return
}

func (i *Interceptor) flushMetric() {
	if len(i.metricMap) == 0 {
		return
	}
	eventbus.PublishOrDrop(eventbus.NormalizeTopic, i.metricMap)
	i.metricMap = make(map[string]*eventbus.NormalizeMetricData)
}
