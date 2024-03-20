package stream

type FilestreamList interface {
	Init() error
	HasScroll() error
	Select() (*File, error)
	Stop()
	GetListLen() int
	IsFinish() (bool, error)
}
