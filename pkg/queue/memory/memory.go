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
	"github.com/smartystreets-prototypes/go-disruptor"
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/batch"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/core/result"
	"loggie.io/loggie/pkg/core/spi"
	"loggie.io/loggie/pkg/pipeline"
	"strings"
	"sync"
	"time"
)

const (
	Type                      = "memory"
	dependencyInterceptorName = "memory-queue-dependency-sink-interceptor"
)

var (
//ringBuffer     api.Event
//ringBufferMask int64
)

func init() {
	pipeline.Register(api.QUEUE, Type, makeQueue)
}

func makeQueue(info pipeline.Info) api.Component {
	return &Queue{
		config:    &Config{},
		epoch:     info.Epoch,
		sinkCount: info.SinkCount,
		listeners: info.R.LoadQueueListeners(),
	}
}

type Queue struct {
	epoch          pipeline.Epoch
	sinkCount      int
	config         *Config
	done           chan struct{}
	name           string
	ringBuffer     []api.Event
	ringBufferMask int64
	ringBufferSize int64
	d              *disruptor.Disruptor
	reservations   int64
	out            chan api.Batch
	listeners      []spi.QueueListener
	countDown      *sync.WaitGroup
}

func (c *Queue) Type() api.Type {
	return Type
}

func (c *Queue) Category() api.Category {
	return api.QUEUE
}

func (c *Queue) Config() interface{} {
	return c.config
}

func (c *Queue) String() string {
	return fmt.Sprintf("%s/%s", api.QUEUE, Type)
}

func (c *Queue) Init(context api.Context) {
	c.done = make(chan struct{})
	c.name = context.Name()
	c.countDown = &sync.WaitGroup{}

	log.Info("sinCount: %d", c.sinkCount)
	if c.config.BatchBufferFactor <= 0 {
		c.config.BatchBufferFactor = c.sinkCount
	}
	if c.config.BatchBufferFactor <= 0 {
		c.config.BatchBufferFactor = 3
	}
	log.Info("%s batch size: %d; batch buffer factor: %d", c.String(),
		c.config.BatchSize, c.config.BatchBufferFactor)
	c.ringBufferSize = int64(c.config.BatchSize * c.config.BatchBufferFactor)
	c.ringBufferMask = c.ringBufferSize - 1
	c.ringBuffer = make([]api.Event, c.ringBufferSize)
	c.reservations = 1
	c.out = make(chan api.Batch, c.sinkCount)

	// init disruptor
	d := disruptor.New(
		disruptor.WithCapacity(c.ringBufferSize),
		disruptor.WithConsumerGroup(newConsumer(c)),
	)
	c.d = &d
}

func (c *Queue) Start() {
	var listeners strings.Builder
	for _, listener := range c.listeners {
		listeners.WriteString(listener.Name())
		listeners.WriteString(" ")
	}
	log.Info("queue listeners: %s", listeners.String())
	go c.startInnerConsumer()
}

func (c *Queue) startInnerConsumer() {
	// inner consumer aka disruptor reader goroutine, done by innerConsumer.Close() callback
	c.countDown.Add(1)
	c.d.Read()
}

func (c *Queue) Stop() {
	if c.d != nil {
		_ = c.d.Close()
	}
	close(c.done)
	c.countDown.Wait()
	c.ringBuffer = nil
	log.Info("queue stop")
}

func (c *Queue) In(event api.Event) {
	c.Consume(event)
}

func (c *Queue) Out() api.Batch {
	return <-c.out
}

func (c *Queue) OutChan() chan api.Batch {
	return c.out
}

func (c *Queue) OutLoop(outFunc api.OutFunc) {
	go c.outLoop(outFunc)
}

func (c *Queue) outLoop(outFunc api.OutFunc) {
	c.countDown.Add(1)
	log.Info("%s out loop start", c.String())
	defer func() {
		c.countDown.Done()
		log.Info("%s out loop stop", c.String())
	}()
	for {
		select {
		case <-c.done:
			return
		case b := <-c.out:
			outFunc(b)
		}
	}
}

func (c *Queue) Consume(event api.Event) api.Result {
	sequence := c.d.Reserve(c.reservations)
	for lower := sequence - c.reservations + 1; lower <= sequence; lower++ {
		c.ringBuffer[lower&c.ringBufferMask] = event
	}
	c.d.Commit(sequence-c.reservations+1, sequence)
	return result.NewResult(api.SUCCESS)
}

func (c *Queue) beforeQueueConvertBatch(events []api.Event) {
	for _, listener := range c.listeners {
		listener.BeforeQueueConvertBatch(events)
	}
}

//func (c *Queue) afterQueueBatchConsumer(batch api.Batch, status api.Status) {
//	for _, listener := range c.listeners {
//		listener.AfterQueueBatchConsumer(batch, status)
//	}
//}

//func (c *Queue) DependencyInterceptors() []api.Interceptor {
//	interceptor := &sink.AbstractInterceptor{
//		DoName: func() string {
//			return dependencyInterceptorName
//		},
//		DoIntercept: func(invoker sink.Invoker, invocation sink.Invocation) api.Result {
//			r := invoker.Invoke(invocation)
//			c.afterQueueBatchConsumer(invocation.Batch, r.Status())
//			return r
//		},
//	}
//	return []api.Interceptor{interceptor}
//}

type innerConsumer struct {
	innerBuffer chan []api.Event
	done        chan struct{}
	out         chan api.Batch
	queue       *Queue
}

func newConsumer(queue *Queue) *innerConsumer {
	batchSize := queue.config.BatchSize
	ic := &innerConsumer{
		innerBuffer: make(chan []api.Event, batchSize),
		done:        queue.done,
		out:         queue.out,
		queue:       queue,
	}
	go ic.run()
	return ic
}

func (ic *innerConsumer) run() {
	ic.queue.countDown.Add(1)
	timeout := ic.queue.config.BatchAggMaxTimeout
	flusher := time.NewTicker(timeout)
	defer func() {
		ic.queue.countDown.Done()
		flusher.Stop()
		log.Info("queue inner consumer stop")
	}()
	batchSize := ic.queue.config.BatchSize
	batchBytes := ic.queue.config.BatchBytes
	size := 0
	bytes := int64(0)
	firstEventAppendTime := time.Now()
	buffer := make([]api.Event, 0, batchSize)
	flush := func() {
		ic.out <- batch.NewBatchWithEvents(buffer)
		buffer = make([]api.Event, 0, batchSize)
		size = 0
		bytes = 0
	}
	for {
		select {
		case <-ic.done:
			return
		case es := <-ic.innerBuffer:
			for _, e := range es {
				if size == 0 {
					firstEventAppendTime = time.Now()
				}
				buffer = append(buffer, e)
				size++
				bytes += int64(len(e.Body()))
				if size >= batchSize || bytes >= batchBytes {
					flush()
				}
			}
		case <-flusher.C:
			// Instead of going to flush directly, check whether the first event of batch append time exceeds the timeout.
			// In order to ensure the integrity of batch as much as possible
			// if size>0, firstEventAppendTime must be updated
			if size > 0 && time.Since(firstEventAppendTime) > timeout {
				flush()
			}
		}
	}
}

func (ic *innerConsumer) Consume(lower, upper int64) {
	batchSize := ic.queue.config.BatchSize
	batchBytes := ic.queue.config.BatchBytes
	ringBuffer := ic.queue.ringBuffer
	ringBufferMask := ic.queue.ringBufferMask
	size := int(upper - lower + 1)
	//log.Info("disruptor-consumer count: %d", size)
	if size < batchSize {
		es := make([]api.Event, 0, size)
		for lower <= upper {
			e := ringBuffer[lower&ringBufferMask]
			lower++
			es = append(es, e)
		}
		ic.queue.beforeQueueConvertBatch(es)
		ic.innerBuffer <- es
	} else {
		es := make([]api.Event, 0, batchSize)
		l := 0
		bytes := int64(0)
		for lower <= upper {
			e := ringBuffer[lower&ringBufferMask]
			lower++

			es = append(es, e)
			l++
			bytes += int64(len(e.Body()))
			if l >= batchSize || bytes >= batchBytes {
				ic.queue.beforeQueueConvertBatch(es)
				ic.out <- batch.NewBatchWithEvents(es)
				es = make([]api.Event, 0, batchSize)
				l = 0
				bytes = 0
			}
		}
		if l > 0 {
			ic.queue.beforeQueueConvertBatch(es)
			ic.innerBuffer <- es
		}
	}
}

// close disruptor will callback
func (ic *innerConsumer) Close() error {
	ic.queue.countDown.Done()
	log.Info("%s inner consumer stop", ic.queue.String())
	return nil
}
