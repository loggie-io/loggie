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
	v, ok := i.MetricContext.MetricMap[process.GetName()]
	if !ok {
		normalizeMetricData := eventbus.NormalizeMetricData{
			BaseInterceptorMetric: eventbus.BaseInterceptorMetric{
				PipelineName:    i.pipelineName,
				InterceptorName: i.name,
			},
			Name:  process.GetName(),
			Count: 1,
		}
		i.MetricContext.MetricMap[process.GetName()] = &normalizeMetricData
		return
	}

	v.Count++
	return
}

func (i *Interceptor) clearMetric() {
	data := eventbus.NormalizeMetricEvent{
		PipelineName: i.MetricContext.PipelineName,
		Name:         i.MetricContext.Name,
		IsClear:      true,
	}
	eventbus.PublishOrDrop(eventbus.NormalizeTopic, &data)
}

func (i *Interceptor) flushMetric() {
	if len(i.MetricContext.MetricMap) == 0 {
		return
	}
	i.MetricContext.IsClear = false
	data := eventbus.NormalizeMetricEvent{
		MetricMap:    make(map[string]*eventbus.NormalizeMetricData),
		PipelineName: i.MetricContext.PipelineName,
		Name:         i.MetricContext.Name,
		IsClear:      false,
	}

	for key, value := range i.MetricContext.MetricMap {
		data.MetricMap[key] = value
	}
	eventbus.PublishOrDrop(eventbus.NormalizeTopic, &data)
	i.MetricContext.MetricMap = make(map[string]*eventbus.NormalizeMetricData)
}
