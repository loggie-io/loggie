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

package grpc

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	pb "github.com/loggie-io/loggie/pkg/sink/grpc/pb"
)

const (
	batchEventIndexKey = event.PrivateKeyPrefix + "BatchEventIndex"
	batchIndexKey      = event.PrivateKeyPrefix + "BatchIndex"
)

var batchIndex uint32

func nextIndex() uint32 {
	return atomic.AddUint32(&batchIndex, 1)
}

type batch struct {
	events     map[int32]api.Event
	start      time.Time
	eventIndex int32
	index      uint32
	timeout    time.Duration
	countDown  sync.WaitGroup
	resp       *pb.LogResp
}

func newBatch(timeout time.Duration) *batch {
	b := &batch{
		events:  make(map[int32]api.Event),
		start:   time.Now(),
		index:   nextIndex(),
		timeout: timeout,
	}
	b.countDown.Add(1)
	return b
}

func (b *batch) size() int {
	return len(b.events)
}

func (b *batch) append(e api.Event) {
	currentIndex := b.eventIndex
	e.Meta().Set(batchEventIndexKey, currentIndex)
	e.Meta().Set(batchIndexKey, b.index)

	b.events[currentIndex] = e
	b.eventIndex++
}

func (b *batch) ack(e api.Event) {
	indexRaw, exist := e.Meta().Get(batchEventIndexKey)
	if !exist {
		return
	}
	index := indexRaw.(int32)
	delete(b.events, index)

	if len(b.events) == 0 {
		// all event ack
		b.resp = &pb.LogResp{
			Success: true,
			Count:   b.eventIndex,
		}
		b.done()
	} else {
		// lazy check timeout
		b.checkTimeout()
	}
}

func (b *batch) checkTimeout() {
	if time.Since(b.start) > b.timeout {
		b.resp = &pb.LogResp{
			Success:  false,
			ErrorMsg: "ack timeout",
		}
		b.done()
	}
}

func (b *batch) done() {
	b.eventIndex = 0
	b.events = nil
	b.countDown.Done()
}

func (b *batch) isDone() bool {
	return b.size() == 0
}

func (b *batch) wait() *pb.LogResp {
	b.countDown.Wait()
	return b.resp
}

type batchChain struct {
	done                chan struct{}
	countDown           sync.WaitGroup
	batchChan           chan *batch
	ackEvents           chan []api.Event
	productFunc         api.ProductFunc
	maintenanceInterval time.Duration
}

func newBatchChain(productFunc api.ProductFunc, maintenanceInterval time.Duration) *batchChain {
	return &batchChain{
		done:                make(chan struct{}),
		batchChan:           make(chan *batch),
		ackEvents:           make(chan []api.Event),
		productFunc:         productFunc,
		maintenanceInterval: maintenanceInterval,
	}
}

func (bc *batchChain) stop() {
	close(bc.done)
	bc.countDown.Wait()
}

func (bc *batchChain) append(b *batch) {
	bc.batchChan <- b
}

func (bc *batchChain) ack(events []api.Event) {
	bc.ackEvents <- events
}

func (bc *batchChain) run() {
	bc.countDown.Add(1)
	bs := make(map[uint32]*batch)
	ticker := time.NewTicker(bc.maintenanceInterval)
	defer func() {
		ticker.Stop()
		bc.countDown.Done()
	}()
	for {
		select {
		case <-bc.done:
			return
		case b := <-bc.batchChan:
			bs[b.index] = b
			for _, e := range b.events {
				bc.productFunc(e)
			}
		case es := <-bc.ackEvents:
			for _, e := range es {
				if batchIndex, ok := e.Meta().Get(batchIndexKey); ok {
					if b, exist := bs[batchIndex.(uint32)]; exist {
						b.ack(e)
						if b.isDone() {
							delete(bs, b.index)
						}
					}
				} else {
					log.Error("event cannot find batchIndex: %s", e.String())
				}
			}
		case <-ticker.C:
			for _, b := range bs {
				b.checkTimeout()
				if b.isDone() {
					delete(bs, b.index)
				}
			}
		}
	}
}
