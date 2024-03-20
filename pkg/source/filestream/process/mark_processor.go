package process

import (
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/source/filestream/process/define"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
	"github.com/loggie-io/loggie/pkg/source/filestream/stream"
)

// MarkProcessor  打标处理器 用来在发送前给上下文打标，从而传递给ack
type MarkProcessor struct {
	next     define.CollectProcessor
	progress storage.CollectProgress
	epoch    *pipeline.Epoch
	source   string
}

func NewMarkProcessor(progress storage.CollectProgress, source string, epoch *pipeline.Epoch) define.CollectProcessor {
	return &MarkProcessor{
		progress: progress,
		epoch:    epoch,
		source:   source,
	}
}

func (m *MarkProcessor) Next(processor define.CollectProcessor) {
	m.next = processor
}

func (m *MarkProcessor) OnError(error) {

}

// OnProcess 打标操作
// 最后一行要打进度，用来落盘
func (m *MarkProcessor) OnProcess(ctx define.Context) {
	if ctx.IsLastLine() {
		ctx.SetAckMark(stream.NewMark(m.progress, m.source, m.epoch, true))
		m.next.OnProcess(ctx)
		return
	}
	ctx.SetAckMark(stream.NewMark(m.progress, m.source, m.epoch, false))
	m.next.OnProcess(ctx)
}
func (m *MarkProcessor) OnComplete(ctx define.Context) {
	ctx.SetAckMark(stream.NewMark(m.progress, m.source, m.epoch, true))
	m.next.OnComplete(ctx)
}
