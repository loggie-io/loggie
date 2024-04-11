package define

// CollectProcessor 采集处理器
type CollectProcessor interface {
	Next(CollectProcessor)
	OnError(error)
	OnProcess(ctx Context)
	OnComplete(ctx Context)
}
