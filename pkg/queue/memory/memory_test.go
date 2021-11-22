/*
Copyright 2021 Loggie Authors

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

package memory

import (
	"fmt"
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/context"
	"loggie.io/loggie/pkg/core/event"
	"loggie.io/loggie/pkg/pipeline"
	"testing"
	"time"
)

func TestQueue_Consume(t *testing.T) {
	queue := makeQueue(pipeline.Info{
		Stop: false,
	}).(api.Queue)
	properties := map[string]interface{}{
		"worker":         4,
		"logAggMaxCount": 1024,
	}
	queue.Init(context.NewContext("memory-queue-1", "memory-queue", api.QUEUE, properties))
	queue.Start()

	go func() {
		for i := 0; i < 1000000; i++ {
			queue.In(event.NewEvent(map[string]interface{}{}, []byte(fmt.Sprintf("test-%v", i))))
		}
	}()

	go func() {
		for {
			batch := queue.Out()
			fmt.Printf("batch: %v \n", batch)
		}
	}()

	time.Sleep(time.Second * 5)
}

func TestQueueBatch(t *testing.T) {
	ss := make([]int, 0, 7)
	for i := 0; i < 7; i++ {
		ss = append(ss, i)
	}
	fmt.Printf("ss(%p,%v)\n", &ss, ss)
	nss := ss[:]
	fmt.Printf("nss(%p,%v)\n", &nss, nss)
	bs := ss[:3]
	fmt.Printf("bs(%p,%v) len: %d, cap: %d\n", &bs, bs, len(bs), cap(bs))
	bs[0] = -1
	fmt.Printf("bs(%p,%v) len: %d, cap: %d\n", &bs, bs, len(bs), cap(bs))
	bs = append(bs, -2)
	fmt.Printf("bs(%p,%v) len: %d, cap: %d\n", &bs, bs, len(bs), cap(bs))
	as := ss[3:]
	fmt.Printf("as(%p,%v) len: %d, cap: %d\n", &as, as, len(as), cap(as))
	as = append(as, 7, 8, 9)
	fmt.Printf("as(%p,%v) len: %d, cap: %d\n", &as, as, len(as), cap(as))
	fmt.Printf("ss(%p,%v)\n", &ss, ss)
}
