package process

import (
	"github.com/loggie-io/loggie/pkg/source/filestream/monitor"
	"github.com/loggie-io/loggie/pkg/source/filestream/process/define"
	"time"
)

type MonitorProcessor struct {
	monitor *monitor.Monitor
	next    define.CollectProcessor
	count   uint64
	begin   int64
}

func NewMonitorProcessor(monitor *monitor.Monitor) define.CollectProcessor {
	return &MonitorProcessor{
		monitor: monitor,
	}
}

func (m *MonitorProcessor) Next(processor define.CollectProcessor) {
	m.next = processor
}

func (m *MonitorProcessor) OnError(error) {

}

func (m *MonitorProcessor) flush() {
	if m.begin == 0 {
		return
	}

	if m.count == 0 {
		return
	}
	m.monitor.CountCollect(m.begin, m.count)
	m.begin = 0
	m.count = 0
}

// OnProcess 打标操作
// 最后一行要打进度，用来落盘
func (m *MonitorProcessor) OnProcess(ctx define.Context) {
	if m.begin == 0 {
		m.begin = time.Now().UnixNano()
	}
	m.count++
	if ctx.IsLastLine() {
		m.flush()
	}
}

// OnComplete 完成通知
func (m *MonitorProcessor) OnComplete(ctx define.Context) {
	m.flush()
}
