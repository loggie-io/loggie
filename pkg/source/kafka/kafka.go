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

package kafka

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/topics"

	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/event"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/pipeline"
)

const Type = "kafka"

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	return &Source{
		done:      make(chan struct{}),
		config:    &Config{},
		eventPool: info.EventPool,
	}
}

type Source struct {
	name      string
	done      chan struct{}
	closeOnce sync.Once
	config    *Config
	consumer  *kafka.Reader
	eventPool *event.Pool
}

func (k *Source) Config() interface{} {
	return k.config
}

func (k *Source) Category() api.Category {
	return api.SOURCE
}

func (k *Source) Type() api.Type {
	return Type
}

func (k *Source) String() string {
	return fmt.Sprintf("%s/%s", api.SOURCE, Type)
}

func (k *Source) Init(context api.Context) {
	k.name = context.Name()
}

func (k *Source) Start() {
	topicRegx, err := regexp.Compile(k.config.Topic)
	if err != nil {
		log.Error("compile kafka topic regex %s error: %s", k.config.Topic, err.Error())
		return
	}

	client := &kafka.Client{
		Addr: kafka.TCP(k.config.Brokers...),
	}
	kts, err := topics.ListRe(context.Background(), client, topicRegx)
	if err != nil {
		log.Error("list kafka topics that match a regex error: %s", err.Error())
		return
	}

	var groupTopics []string
	for _, t := range kts {
		groupTopics = append(groupTopics, t.Name)
	}
	if len(groupTopics) <= 0 {
		log.Error("regex %s matched zero kafka topics", k.config.Topic)
		return
	}

	readerCfg := kafka.ReaderConfig{
		Brokers:        k.config.Brokers,
		GroupID:        k.config.GroupId,
		GroupTopics:    groupTopics,
		CommitInterval: time.Second * time.Duration(k.config.AutoCommitInterval),
		StartOffset:    getAutoOffset(k.config.AutoOffsetReset),
	}

	k.consumer = kafka.NewReader(readerCfg)
}

func (k *Source) Stop() {
	k.closeOnce.Do(func() {
		if k.consumer != nil {
			err := k.consumer.Close()
			if err != nil {
				log.Panic("close kafka consumer error: %+v", err)
			}
		}

		close(k.done)
	})
}

func (k *Source) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", k.String())

	for {
		select {
		case <-k.done:
			return

		default:
			err := k.consume(productFunc)
			if err != nil {
				log.Error("%+v", err)
			}
		}
	}
}

func (k *Source) consume(productFunc api.ProductFunc) error {
	if k.consumer == nil {
		return fmt.Errorf("kakfa consumer not initialized yet")
	}

	msg, err := k.consumer.ReadMessage(context.Background())
	if err != nil {
		return errors.Errorf("consumer read message error: %v", err)
	}

	e := k.eventPool.Get()
	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}
	header["kafka"] = map[string]interface{}{
		"offset":    msg.Offset,
		"partition": msg.Partition,
		"timestamp": msg.Time.Format(time.RFC3339),
		"topic":     msg.Topic,
	}

	for _, h := range msg.Headers {
		header[h.Key] = string(h.Value)
	}
	header["@timestamp"] = time.Now().Format(time.RFC3339)
	e.Fill(e.Meta(), header, msg.Value)
	productFunc(e)
	return nil
}

func (k *Source) Commit(events []api.Event) {
	k.eventPool.PutAll(events)
}
