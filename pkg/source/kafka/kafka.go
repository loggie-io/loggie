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
	"net"
	"regexp"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/topics"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	kafkaSink "github.com/loggie-io/loggie/pkg/sink/kafka"
)

const (
	Type = "kafka"

	fKafka     = "kafka"
	fOffset    = "offset"
	fPartition = "partition"
	fTimestamp = "timestamp"
	fTopic     = "topic"
)

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

func (k *Source) Init(context api.Context) error {
	k.name = context.Name()
	return nil
}

func (k *Source) Start() error {
	c := k.config
	mechanism, err := kafkaSink.Mechanism(c.SASL.Type, c.SASL.UserName, c.SASL.Password, c.SASL.Algorithm)
	if err != nil {
		log.Error("kafka source sasl mechanism with error: %s", err.Error())
		return err
	}

	topicRegx, err := regexp.Compile(k.config.Topic)
	if err != nil {
		log.Error("compile kafka topic regex %s error: %s", k.config.Topic, err.Error())
		return err
	}

	client := &kafka.Client{
		Addr: kafka.TCP(k.config.Brokers...),
		Transport: &kafka.Transport{
			Dial: (&net.Dialer{
				Timeout: 3 * time.Second,
			}).DialContext,
			SASL: mechanism,
		},
	}
	kts, err := topics.ListRe(context.Background(), client, topicRegx)
	if err != nil {
		return errors.WithMessage(err, "list kafka topics that match a regex error")
	}

	var groupTopics []string
	for _, t := range kts {
		groupTopics = append(groupTopics, t.Name)
	}
	if len(groupTopics) <= 0 {
		return errors.Errorf("regex %s could not match any kafka topics", k.config.Topic)
	}

	dial := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
	}
	if k.config.ClientId != "" {
		dial.ClientID = k.config.ClientId
	}
	readerCfg := kafka.ReaderConfig{
		Brokers:        k.config.Brokers,
		GroupID:        k.config.GroupId,
		GroupTopics:    groupTopics,
		QueueCapacity:  k.config.QueueCapacity,
		MinBytes:       k.config.MinAcceptedBytes,
		MaxBytes:       k.config.MaxAcceptedBytes,
		MaxAttempts:    k.config.ReadMaxAttempts,
		MaxWait:        k.config.MaxReadWait,
		ReadBackoffMin: k.config.ReadBackoffMin,
		ReadBackoffMax: k.config.ReadBackoffMax,
		CommitInterval: k.config.AutoCommitInterval,
		StartOffset:    getAutoOffset(k.config.AutoOffsetReset),
		Dialer:         dial,
	}

	k.consumer = kafka.NewReader(readerCfg)
	return nil
}

func (k *Source) Stop() {
	k.closeOnce.Do(func() {
		close(k.done)

		if k.consumer != nil {
			err := k.consumer.Close()
			if err != nil {
				log.Error("close kafka consumer error: %+v", err)
			}
		}
	})
}

func (k *Source) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", k.String())

	if k.consumer == nil {
		log.Error("kafka consumer not initialized yet")
		return
	}

	wg := sync.WaitGroup{}
	for i := 0; i < k.config.Worker; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
		}()
	}

	wg.Wait()
}

func (k *Source) consume(productFunc api.ProductFunc) error {
	ctx := context.Background()
	msg, err := k.consumer.FetchMessage(ctx)
	if err != nil {
		return errors.Errorf("consumer read message error: %v", err)
	}

	e := k.eventPool.Get()
	header := e.Header()
	if k.config.AddonMeta != nil && *k.config.AddonMeta == true {
		if header == nil {
			header = make(map[string]interface{})
		}

		// set default metadata
		header[fKafka] = map[string]interface{}{
			fOffset:    msg.Offset,
			fPartition: msg.Partition,
			fTimestamp: msg.Time.Format(time.RFC3339),
			fTopic:     msg.Topic,
		}
		for _, h := range msg.Headers {
			header[h.Key] = string(h.Value)
		}
	}

	meta := e.Meta()
	if k.config.EnableAutoCommit {
		// auto commit message, commit before sink ack
		if err := k.consumer.CommitMessages(ctx, msg); err != nil {
			return errors.Errorf("consumer auto commit message error: %v", err)
		}

	} else {
		// set fields to metadata for kafka commit message
		if meta == nil {
			meta = event.NewDefaultMeta()
		}
		meta.Set(fOffset, msg.Offset)
		meta.Set(fPartition, msg.Partition)
		meta.Set(fTopic, msg.Topic)
	}

	e.Fill(meta, header, msg.Value)

	productFunc(e)
	return nil
}

func (k *Source) Commit(events []api.Event) {
	// commit when sink ack
	if !k.config.EnableAutoCommit {
		var msgs []kafka.Message
		for _, e := range events {
			meta := e.Meta()

			var mKafka, mPartition, mOffset interface{}
			mKafka, exist := meta.Get(fKafka)
			if !exist {
				continue
			}
			mPartition, exist = meta.Get(fPartition)
			if !exist {
				continue
			}
			mOffset, exist = meta.Get(fOffset)
			if !exist {
				continue
			}

			msgs = append(msgs, kafka.Message{
				Topic:     mKafka.(string),
				Partition: mPartition.(int),
				Offset:    mOffset.(int64),
			})
		}
		if len(msgs) > 0 {
			err := k.consumer.CommitMessages(context.Background(), msgs...)
			if err != nil {
				log.Error("consumer manually commit message error: %v", err)
			}
		}
	}

	k.eventPool.PutAll(events)
}
