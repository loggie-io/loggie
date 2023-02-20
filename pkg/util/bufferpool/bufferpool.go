/*
Copyright 2022 Loggie Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bufferpool

import (
	"bytes"
	"sync"
)

type BufferPool struct {
	sync.Pool
}

func NewBufferPool(s int) *BufferPool {
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
