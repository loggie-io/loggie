/*
Copyright 2023 Loggie Authors

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

package ack

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/queue/persisted/disk"
)

const largeAckSize = 10 * 4096

// After the sink is sent successfully,
// it is also necessary to persist the offset record of the consumption queue file
// for the next consumption after restarting.

type Chain struct {
	appendChan chan *disk.ReadAck
	ackChan    chan *disk.ReadAck
	done       chan struct{}

	// All sent values will be recorded in the map and added to the linked list,
	// When the sink sends successfully and gets ack, it will mark that it has been sent successfully,
	// and at the same time delete all linked list nodes that have been sent successfully.
	tail   *ack
	allAck map[BatchUID]*ack

	CommitFunc CommitFunc
}

func NewAckChain() *Chain {
	chain := &Chain{
		appendChan: make(chan *disk.ReadAck),
		ackChan:    make(chan *disk.ReadAck),
		done:       make(chan struct{}),
		allAck:     make(map[BatchUID]*ack),
	}

	return chain
}

// CommitFunc to persist the progress of the consumption queue
type CommitFunc func(readPos int64, readFileNum int64)

type BatchUID string

func GenBatchUID(readPos int64, readFileNum int64) BatchUID {
	return BatchUID(fmt.Sprintf("%d-%d", readPos, readFileNum))
}

// Pos progress of reading the persistent file
type Pos struct {
	readPos     int64
	readFileNum int64
}

func NewPos(readPos int64, readFileNum int64) *Pos {
	return &Pos{
		readPos:     readPos,
		readFileNum: readFileNum,
	}
}

type ack struct {
	next *ack

	// Is it the first node of the linked list
	first bool

	// if the batch has been sent successfully
	done bool

	uid         BatchUID
	readPos     int64
	readFileNum int64
}

func newAck(readPos int64, readFileNum int64) *ack {
	// TODO with sync.Pool
	return &ack{
		readPos:     readPos,
		readFileNum: readFileNum,
		uid:         GenBatchUID(readPos, readFileNum),
	}
}

func (a *Chain) Run() {
	for {
		select {
		case <-a.done:
			return

		case bs := <-a.appendChan:
			if bs != nil {
				a.appendBatch(bs.ReadPos, bs.ReadFileNum)
			}

		case bc := <-a.ackChan:
			if bc != nil {
				a.ackBatch(bc.ReadPos, bc.ReadFileNum)
			}
		}
	}
}

func (a *Chain) Stop() {
	close(a.done)
}

// Append to sending ack linked list
func (a *Chain) Append(p *disk.ReadAck) {
	a.appendChan <- p
}

// Ack confirm that it has been sent successfully on the sink side
func (a *Chain) Ack(p *disk.ReadAck) {
	a.ackChan <- p
}

func (a *Chain) appendBatch(readPos int64, readFileNum int64) {
	ackItem := newAck(readPos, readFileNum)

	existAck, ok := a.allAck[ackItem.uid]
	if ok {
		log.Error("append state(%+v) exist: %#v", existAck)
		return
	}

	a.allAck[ackItem.uid] = ackItem
	if a.tail == nil {
		ackItem.first = true
		a.tail = ackItem
		return
	}

	a.tail.next = ackItem
	a.tail = ackItem

	// Check whether the chain is too long
	l := len(a.allAck)
	if l > largeAckSize {
		log.Error("AckChain is too long")
		return
	}
}

func (a *Chain) ackBatch(readPos int64, readFileNum int64) {
	ackedBatch, ok := a.allAck[GenBatchUID(readPos, readFileNum)]
	if !ok {
		log.Error("cannot find acked batch: %#v", ackedBatch)
		return
	}

	ackedBatch.done = true
	if ackedBatch.first {
		a.ackLoop(ackedBatch)
	}
}

// Loop through and delete records that have been successfully sent
func (a *Chain) ackLoop(first *ack) {
	next := first
	prev := first

	for next != nil {
		if !next.done {
			next.first = true
			break
		}

		if a.tail.uid == next.uid {
			a.tail = nil
		}

		delete(a.allAck, next.uid)

		prev = next
		next = next.next
		// delete previous node
		prev.next = nil
	}

	// Persist ack into the queue
	a.CommitFunc(prev.readPos, prev.readFileNum)
}
