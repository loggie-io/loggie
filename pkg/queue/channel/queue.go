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

package channel

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/batch"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/spi"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

const (
	Type = "channel"
)

func init() {
	pipeline.Register(api.QUEUE, Type, makeQueue)
}

func makeQueue(info pipeline.Info) api.Component {
	return &Queue{
		config:       &Config{},
		pipelineName: info.PipelineName,
		epoch:        info.Epoch,
		sinkCount:    info.SinkCount,
		listeners:    info.R.LoadQueueListeners(),
	}
}

type Queue struct {
	pipelineName string
	epoch        pipeline.Epoch
	sinkCount    int
	config       *Config
	done         chan struct{}
	name         string
	in           chan api.Event
	out          chan api.Batch
	listeners    []spi.QueueListener
	countDown    *sync.WaitGroup
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
	c.out = make(chan api.Batch, c.sinkCount)
	//c.in = make(chan api.Event, c.config.BatchSize*c.config.BatchBufferFactor)
	c.in = make(chan api.Event, 16)
}

func (c *Queue) Start() {
	var listeners strings.Builder
	for _, listener := range c.listeners {
		listeners.WriteString(listener.Name())
		listeners.WriteString(" ")
	}
	log.Info("queue listeners: %s", listeners.String())
	go c.worker()
}

func (c *Queue) worker() {
	c.countDown.Add(1)
	log.Info("channel queue worker start")
	timeout := c.config.BatchAggMaxTimeout
	flusher := time.NewTicker(timeout)
	defer func() {
		flusher.Stop()
		c.countDown.Done()
		log.Info("channel queue(%s) worker stop", c.String())
	}()
	firstEventAppendTime := time.Now()
	batchBytes := c.config.BatchBytes
	batchSize := c.config.BatchSize
	buffer := make([]api.Event, 0, batchSize)
	size := 0
	bytes := int64(0)
	flush := func() {
		c.beforeQueueConvertBatch(buffer)
		c.out <- batch.NewBatchWithEvents(buffer)
		buffer = make([]api.Event, 0, batchSize)
		size = 0
		bytes = 0
	}
	for {
		select {
		case <-c.done:
			eventbus.PublishOrDrop(eventbus.QueueMetricTopic, eventbus.QueueMetricData{
				PipelineName: c.pipelineName,
				Type:         string(c.Type()),
				Capacity:     int64(batchSize),
				Size:         int64(size),
			})
			return
		case e := <-c.in:
			if size == 0 {
				firstEventAppendTime = time.Now()
			}
			buffer = append(buffer, e)
			size++
			bytes += int64(len(e.Body()))
			if size >= batchSize || bytes >= batchBytes {
				flush()
			}
		case <-flusher.C:
			// Instead of going to flush directly, check whether the first event of batch append time exceeds the timeout.
			// In order to ensure the integrity of batch as much as possible
			// if size>0, firstEventAppendTime must be updated
			if size > 0 && time.Since(firstEventAppendTime) > timeout {
				flush()
			}
			eventbus.PublishOrDrop(eventbus.QueueMetricTopic, eventbus.QueueMetricData{
				PipelineName: c.pipelineName,
				Type:         string(c.Type()),
				Capacity:     int64(batchSize),
				Size:         int64(size),
			})
		}
	}
}

func (c *Queue) Stop() {
	close(c.done)
	c.countDown.Wait()
	log.Info("[%s]channel queue stop", c.pipelineName)
}

func (c *Queue) In(event api.Event) {
	c.in <- event
}

func (c *Queue) Out() api.Batch {
	return <-c.out
}

func (c *Queue) OutChan() chan api.Batch {
	return c.out
}

func (c *Queue) beforeQueueConvertBatch(events []api.Event) {
	for _, listener := range c.listeners {
		listener.BeforeQueueConvertBatch(events)
	}
}
