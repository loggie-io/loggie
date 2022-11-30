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

	newBatchOpt    = optType(0)
	appendEventOpt = optType(1)
	ackEventOpt    = optType(2)
	finishBatchOpt = optType(3)
)

var (
	serverStopResponse = &pb.LogResp{
		Success:  false,
		ErrorMsg: "server stop",
	}

	serverProcessTimeout = &pb.LogResp{
		Success:  false,
		ErrorMsg: "ack timeout",
	}

	serverProcessSuccessFunc = func(eventIndex int32) *pb.LogResp {
		return &pb.LogResp{
			Success: true,
			Count:   eventIndex,
		}
	}
)

var batchIndex uint32

func nextIndex() uint32 {
	return atomic.AddUint32(&batchIndex, 1)
}

type batch struct {
	finish     bool
	eventIndex uint32
	eventMarks map[uint32]bool
	size       int32
	start      time.Time
	index      uint32
	timeout    time.Duration
	respChan   chan *pb.LogResp
	bc         *batchChain
}

func (b *batch) Append(e api.Event) {
	e.Meta().Set(batchIndexKey, b.index)
	currentEventIndex := b.eventIndex
	b.eventIndex++
	e.Meta().Set(batchEventIndexKey, currentEventIndex)

	o := opt{
		o:  appendEventOpt,
		bi: b.index,
		ei: currentEventIndex,
	}
	b.bc.optChan <- o
}

func (b *batch) ack(eventIndex uint32) bool {
	delete(b.eventMarks, eventIndex)
	return b.checkDoneOrTimeout()
}

func (b *batch) checkDoneOrTimeout() bool {
	if !b.finish {
		return false
	}

	if len(b.eventMarks) == 0 {
		// all event ack
		b.respChan <- serverProcessSuccessFunc(b.size)
		return true
	}

	if time.Since(b.start) > b.timeout {
		b.respChan <- serverProcessTimeout
		return true
	}
	return false
}

func (b *batch) Wait() *pb.LogResp {
	b.bc.optChan <- opt{
		o:  finishBatchOpt,
		bi: b.index,
	}
	select {
	case <-b.bc.done:
		return serverStopResponse
	case r := <-b.respChan:
		return r
	}
}

type optType int32

type opt struct {
	o  optType
	b  *batch
	bi uint32
	ei uint32
}

type batchChain struct {
	done                chan struct{}
	optChan             chan opt
	countDown           sync.WaitGroup
	maintenanceInterval time.Duration
}

func newBatchChain(maintenanceInterval time.Duration) *batchChain {
	return &batchChain{
		done:                make(chan struct{}),
		optChan:             make(chan opt),
		maintenanceInterval: maintenanceInterval,
	}
}

func (bc *batchChain) stop() {
	close(bc.done)
	bc.countDown.Wait()
}

func (bc *batchChain) NewBatch(timeout time.Duration) *batch {
	b := &batch{
		eventMarks: make(map[uint32]bool),
		start:      time.Now(),
		index:      nextIndex(),
		timeout:    timeout,
		bc:         bc,
		respChan:   make(chan *pb.LogResp, 1),
	}
	o := opt{
		o: newBatchOpt,
		b: b,
	}
	bc.optChan <- o
	return b
}

func (bc *batchChain) ack(events []api.Event) {
	for _, e := range events {
		index, eventIndex := bc.parseIndex(e)
		if index < 0 || eventIndex < 0 {
			log.Error("event cannot find batchIndex or batchEventIndex: %s; meta: %s", e.String(), e.Meta().String())
			continue
		}
		o := opt{
			o:  ackEventOpt,
			bi: uint32(index),
			ei: uint32(eventIndex),
		}
		select {
		case <-bc.done:
			return
		case bc.optChan <- o:
		}
	}
}

func (bc *batchChain) run() {
	bc.countDown.Add(1)
	log.Info("grpc batch chain start")
	bs := make(map[uint32]*batch)
	ticker := time.NewTicker(bc.maintenanceInterval)
	defer func() {
		ticker.Stop()
		bc.countDown.Done()
		log.Info("grpc batch chain stop")
	}()
	for {
		select {
		case <-bc.done:
			return
		case o := <-bc.optChan:
			t := o.o
			if t == newBatchOpt {
				b := o.b
				bs[b.index] = b
			} else if t == appendEventOpt {
				b := bs[o.bi]
				b.eventMarks[o.ei] = false
				b.size++
			} else if t == ackEventOpt {
				// may be removed prematurely due to timeout
				if b, exist := bs[o.bi]; exist {
					if b.ack(o.ei) {
						delete(bs, b.index)
					}
				}
			} else if t == finishBatchOpt {
				index := o.bi
				b := bs[index]
				b.finish = true
				if b.checkDoneOrTimeout() {
					delete(bs, b.index)
				}
			}
		case <-ticker.C:
			for _, b := range bs {
				if b.checkDoneOrTimeout() {
					delete(bs, b.index)
				}
			}
		}
	}
}

func (bc *batchChain) parseIndex(e api.Event) (batchIndex, batchEventIndex int32) {
	{
		bi, exist := e.Meta().Get(batchIndexKey)
		if !exist {
			batchIndex = -1
		} else {
			batchIndex = int32(bi.(uint32))
		}
	}

	{
		ei, exist := e.Meta().Get(batchEventIndexKey)
		if !exist {
			batchEventIndex = -1
		} else {
			batchEventIndex = int32(ei.(uint32))
		}
	}
	return batchIndex, batchEventIndex
}
