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

package persisted

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/batch"
	"github.com/loggie-io/loggie/pkg/queue/persisted/ack"
	"github.com/loggie-io/loggie/pkg/queue/persisted/disk"
	fileutils "github.com/loggie-io/loggie/pkg/util/file"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/spi"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

const (
	Type = "persisted"

	systemMetaReadPos     = "readPos"
	systemMetaReadFileNum = "readFileNum"
)

func init() {
	pipeline.Register(api.QUEUE, Type, makeQueue)
}

func makeQueue(info pipeline.Info) api.Component {
	return &Queue{
		config:       &Config{},
		pipelineName: info.PipelineName,
		sinkCount:    info.SinkCount,
		listeners:    info.R.LoadQueueListeners(),
	}
}

type Queue struct {
	pipelineName string
	sinkCount    int
	config       *Config
	done         chan struct{}
	name         string
	in           chan api.Event
	out          chan api.Batch
	listeners    []spi.QueueListener
	countDown    *sync.WaitGroup

	sources map[string]api.Source // key:name|value:source

	disk     disk.Interface
	ackChain *ack.Chain

	storePath string
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

func (c *Queue) Init(context api.Context) error {
	c.done = make(chan struct{})
	c.name = context.Name()
	c.countDown = &sync.WaitGroup{}

	log.Info("sink count: %d", c.sinkCount)
	log.Info("%s batch size: %d", c.String(),
		c.config.BatchSize)
	c.out = make(chan api.Batch, c.sinkCount)
	c.in = make(chan api.Event, 16)

	c.ackChain = ack.NewAckChain()
	c.ackChain.CommitFunc = func(readPos int64, readFileNum int64) {
		c.disk.Commit(readPos, readFileNum)
	}

	// The stored path is added with the pipeline name as a subpath
	c.storePath = filepath.Join(c.config.Path, c.pipelineName)
	if err := fileutils.CreateDirIfNotExist(c.storePath); err != nil {
		return err
	}

	c.disk = disk.New(c.pipelineName, c.storePath, c.config.MaxFileBytes.Bytes,
		1, int32(c.config.MaxFileBytes.Bytes), c.config.SyncCount, c.config.SyncTimeout, c.config.MaxFileCount)

	return nil
}

func (c *Queue) Start() error {
	var listeners strings.Builder
	for _, listener := range c.listeners {
		listeners.WriteString(listener.Name())
		listeners.WriteString(" ")
	}

	go c.ackChain.Run()
	go c.writerLoop()
	go c.readLoop()

	log.Info("persisted queue %s started, file path: %s", c.pipelineName, c.storePath)
	return nil
}

func (c *Queue) writerLoop() {
	c.countDown.Add(1)
	log.Info("persisted queue worker start")
	timeout := c.config.BatchAggMaxTimeout
	flusher := time.NewTicker(timeout)
	defer func() {
		flusher.Stop()
		c.countDown.Done()
		log.Info("persisted queue(%s) worker stop", c.String())
	}()
	firstEventAppendTime := time.Now()
	batchBytes := c.config.BatchBytes
	batchSize := c.config.BatchSize
	buffer := make([]api.Event, 0, batchSize)
	size := 0
	bytes := int64(0)
	flush := func() {
		c.beforeQueueConvertBatch(buffer)

		b := batch.NewBatchWithEvents(buffer)
		//w, err := b.JsonMarshal()
		w, err := b.MsgPackMarshal()
		if err != nil {
			log.Error("marshal batch to persisted queue failed: %+v", err)
			return
		}

		if err := c.disk.Put(w); err != nil {
			log.Error("write batch to persisted queue failed: %+v", err)
		}

		c.releaseEvents(buffer)

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
			if size >= batchSize || bytes >= batchBytes.Bytes {
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

func (c *Queue) readLoop() {
	for {
		select {
		case <-c.done:
			return

		case readFrame := <-c.disk.ReadChan():
			if readFrame == nil {
				continue
			}
			//b, err := batch.JsonUnmarshal(readFrame.Raw)
			b, err := batch.MsgPackUnmarshal(readFrame.Raw)
			if err != nil {
				log.Error("unmarshal batch from persisted queue failed: %+w", err)
				return
			}

			b.Meta()[systemMetaReadPos] = readFrame.ReadPos
			b.Meta()[systemMetaReadFileNum] = readFrame.ReadFileNum
			c.ackChain.Append(&disk.ReadAck{ReadPos: readFrame.ReadPos, ReadFileNum: readFrame.ReadFileNum})
			c.out <- b
		}
	}
}

func (c *Queue) Stop() {
	c.disk.Close()
	c.ackChain.Stop()
	close(c.done)
	c.countDown.Wait()
	log.Info("[%s]persisted queue stop", c.pipelineName)
}

func (c *Queue) In(event api.Event) {
	c.in <- event
}

func (c *Queue) Out() chan api.Batch {
	return c.out
}

func (c *Queue) beforeQueueConvertBatch(events []api.Event) {
	for _, listener := range c.listeners {
		listener.BeforeQueueConvertBatch(events)
	}
}

// Commit after the sink is sent successfully, the returned ack offset is recorded
func (c *Queue) Commit(batch api.Batch) {
	readPos := batch.Meta()[systemMetaReadPos]
	readFileNum := batch.Meta()[systemMetaReadFileNum]
	c.ackChain.Ack(&disk.ReadAck{ReadPos: readPos.(int64), ReadFileNum: readFileNum.(int64)})
}

// SetSource injected into the pipeline for commit
func (c *Queue) SetSource(src map[string]api.Source) {
	c.sources = src
}

// Write queue successfully, commit to source
func (c *Queue) releaseEvents(events []api.Event) {
	nes := make(map[string][]api.Event)
	l := len(events)
	for _, e := range events {
		sourceName := e.Meta().Source()
		es, ok := nes[sourceName]
		if !ok {
			es = make([]api.Event, 0, l)
		}
		es = append(es, e)
		nes[sourceName] = es
	}
	for sn, es := range nes {
		c.sources[sn].Commit(es)
	}
}
