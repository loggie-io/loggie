package process

import (
	"github.com/loggie-io/loggie/pkg/source/filestream/process/define"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
)

// ChainProcessor 管理处理链条，在收到数据的时候将处理链条上的处理器依次执行
// 链条处理整体进度
// line -> decoder  -> first -> mark -> progress -> transporter
type ChainProcessor struct {
	progress storage.CollectProgress
	// begin 头结点
	begin define.CollectProcessor
	// current 当前节点
	current define.CollectProcessor
}

// NewChainProcessor 初始化链条
func NewChainProcessor(progress storage.CollectProgress) define.ChainProcess {
	return &ChainProcessor{
		progress: progress,
	}
}

func (c *ChainProcessor) GetProgress() storage.CollectProgress {
	return c.progress
}

// AddProcessor 添加处理器
func (c *ChainProcessor) AddProcessor(processor define.CollectProcessor) {

	if processor == nil {
		return
	}

	if c.begin == nil {
		c.begin = processor
		c.current = processor
		return
	}
	c.current.Next(processor)
	c.current = processor
}

// OnProcess 性能敏感模块
// 尽量减少内存copy，赋值转换操作都要少做，避免出现隐式内存拷贝
func (c *ChainProcessor) OnProcess(ctx define.Context) {
	c.begin.OnProcess(ctx)
}

func (c *ChainProcessor) OnError(err error) {
	c.begin.OnError(err)
}

func (c *ChainProcessor) OnComplete(ctx define.Context) {
	c.begin.OnComplete(ctx)
}
