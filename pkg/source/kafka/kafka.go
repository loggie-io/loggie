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
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/event"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/pipeline"
	"sync"
	"time"
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
	consumer  *kafka.Consumer
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
	conf := &kafka.ConfigMap{
		cBrokers:          k.config.Brokers,
		cGroupId:          k.config.GroupId,
		cEnableAutoCommit: k.config.EnableAutoCommit,
		cAutoOffsetReset:  k.config.AutoOffsetReset,
	}
	var err error
	if k.config.AutoCommitInterval != 0 {
		err = conf.SetKey(cAutoCommitInterval, k.config.AutoCommitInterval)
	}
	if err != nil {
		log.Panic("set config error: %+v", err)
	}

	c, err := kafka.NewConsumer(conf)
	if err != nil {
		log.Panic("init kafka consumer error: %+v", err)
	}
	err = c.SubscribeTopics(k.config.Topics, nil)
	if err != nil {
		log.Panic("subscribe topics %+v error: %+v", k.config.Topics, err)
	}
	k.consumer = c
}

func (k *Source) Stop() {
	k.closeOnce.Do(func() {
		err := k.consumer.Close()
		if err != nil {
			log.Panic("close kafka consumer error: %+v", err)
		}

		close(k.done)
	})

}

func (k *Source) Product() api.Event {
	return nil
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
	msg, err := k.consumer.ReadMessage(-1)
	if err != nil {
		return errors.Errorf("consumer read message error: %v", err)
	}
	if msg == nil {
		return errors.New("received message is nil")
	}

	e := k.eventPool.Get()
	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}
	header["kafka"] = map[string]interface{}{
		"offset":    msg.TopicPartition.Offset,
		"partition": msg.TopicPartition.Partition,
		"timestamp": msg.Timestamp.Format(time.RFC3339),
		"topic":     msg.TopicPartition.Topic,
	}

	for _, h := range msg.Headers {
		header[h.Key] = string(h.Value)
	}
	header["@timestamp"] = time.Now().Format(time.RFC3339)
	e.Fill(header, msg.Value)
	productFunc(e)
	return nil
}

func (k *Source) Commit(events []api.Event) {
	k.eventPool.PutAll(events)
}
