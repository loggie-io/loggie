package monitor

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"sync"
)

type MetricReporter struct {
	pipeLineName string
	sourceName   string
	isStop       bool
	mutex        sync.RWMutex
}

// ZL: 保存上下文，用于导出metrics
type ProducerHandleStreamContext struct {
	streamName    string
	fields        map[string]interface{}
	productEvents uint64
	// 生产耗时
	duration uint64
	// 扫描耗时
	scanDuration uint64
	FilesCount   uint32
}

func NewMetricReporter(pipelineName string, sourceName string) *MetricReporter {
	return &MetricReporter{
		pipeLineName: pipelineName,
		sourceName:   sourceName,
		isStop:       false,
	}
}

// ZL: 上报metrics
func (exporter *MetricReporter) reportMetric(context *eventbus.FileStreamCollectMetricData) {
	exporter.mutex.RLock()
	defer exporter.mutex.RUnlock()
	if exporter.isStop == true {
		log.Debug("metric reporter has stop:%s-%s", exporter.sourceName, exporter.pipeLineName)
		return
	}
	context.BaseMetric.PipelineName = exporter.pipeLineName
	context.BaseMetric.SourceName = exporter.sourceName
	eventbus.PublishOrDrop(eventbus.FileStreamMetricTopic, *context)
}

func (exporter *MetricReporter) StopMetric() {
	exporter.mutex.Lock()
	defer exporter.mutex.Unlock()
	exporter.isStop = true
	context := eventbus.FileStreamCollectMetricData{
		BaseMetric: eventbus.BaseMetric{
			SourceName:   exporter.sourceName,
			PipelineName: exporter.pipeLineName,
		},
		Done: true,
	}
	eventbus.PublishOrDrop(eventbus.FileStreamMetricTopic, context)
	log.Info("producer->stopMetric")
}
