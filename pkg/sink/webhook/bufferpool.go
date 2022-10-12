package webhook

import (
	"bytes"
	"sync"
)

type BufferPool struct {
	sync.Pool
}

func newBufferPool(s int) *BufferPool {
	return &BufferPool{
		Pool: sync.Pool{
			New: func() interface{} {
				b := bytes.NewBuffer(make([]byte, s))
				b.Reset()
				return b
			},
		},
	}
}

func (bp *BufferPool) Get() *bytes.Buffer {
	return bp.Pool.Get().(*bytes.Buffer)
}

func (bp *BufferPool) Put(b *bytes.Buffer) {
	b.Reset()
	bp.Pool.Put(b)
}
