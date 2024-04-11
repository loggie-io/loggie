package process

import (
	"github.com/loggie-io/loggie/pkg/source/filestream/process/define"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
)

// Progress   采集进度处理器 用来在发送前给上下文打标，从而传递给ack
type Progress struct {
	next     define.CollectProcessor
	progress storage.CollectProgress
}

func NewProgressProcessor(progress storage.CollectProgress) define.CollectProcessor {
	return &Progress{
		progress: progress,
	}
}

func (m *Progress) Next(processor define.CollectProcessor) {
	m.next = processor
}

func (m *Progress) OnError(error) {

}

func (m *Progress) move(ctx define.Context) {
	if len(ctx.GetBuf()) == 0 {
		return
	}
	m.progress.SetCollectHash(ctx.GetHash())
	m.progress.AddCollectSeq(1)

	m.progress.AddCollectOffset(len(ctx.GetBuf()) + int(ctx.GetEndLineLength()))
}

func (m *Progress) OnProcess(ctx define.Context) {
	m.move(ctx)
	m.next.OnProcess(ctx)
}
func (m *Progress) OnComplete(ctx define.Context) {
	if len(ctx.GetBuf()) != 0 {
		m.move(ctx)
	}
	m.next.OnComplete(ctx)
}
