package define

import "github.com/loggie-io/loggie/pkg/source/filestream/storage"

type ChainProcess interface {
	// AddProcessor 添加一个处理器
	AddProcessor(processor CollectProcessor)
	// OnError 出错
	OnError(error)
	// OnProcess 处理流
	OnProcess(ctx Context)
	// OnComplete 完成一个流采集
	OnComplete(ctx Context)
	// GetProgress 获取采集进度
	GetProgress() storage.CollectProgress
}
