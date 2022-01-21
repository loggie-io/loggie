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
	"github.com/loggie-io/loggie/pkg/core/source/abstract"
	"regexp"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/topics"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

const Type = "kafka"

func init() {
	abstract.SourceRegister(Type, makeSource)
}

func makeSource(info pipeline.Info) abstract.SourceConvert {
	return &Source{
		done:   make(chan struct{}),
		config: &Config{},
	}
}

type Source struct {
	*abstract.Source
	done      chan struct{}
	closeOnce sync.Once
	config    *Config
	consumer  *kafka.Reader
}

func (k *Source) Config() interface{} {
	return k.config
}

func (k *Source) DoStart() {
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
		QueueCapacity:  k.config.QueueCapacity,
		MinBytes:       k.config.MinAcceptedBytes,
		MaxBytes:       k.config.MaxAcceptedBytes,
		MaxAttempts:    k.config.ReadMaxAttempts,
		MaxWait:        k.config.MaxReadWait,
		ReadBackoffMin: k.config.ReadBackoffMin,
		ReadBackoffMax: k.config.ReadBackoffMax,
		CommitInterval: k.config.AutoCommitInterval,
		StartOffset:    getAutoOffset(k.config.AutoOffsetReset),
	}

	k.consumer = kafka.NewReader(readerCfg)
}

func (k *Source) DoStop() {
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

func (k *Source) DoProduct() {
	log.Info("%s start product loop", k.String())

	for {
		select {
		case <-k.done:
			return

		default:
			err := k.consume()
			if err != nil {
				log.Error("%+v", err)
			}
		}
	}
}

func (k *Source) consume() error {
	if k.consumer == nil {
		return fmt.Errorf("kakfa consumer not initialized yet")
	}

	ctx := context.Background()
	msg, err := k.consumer.FetchMessage(ctx)
	if err != nil {
		return errors.Errorf("consumer read message error: %v", err)
	}

	// auto commit message, commit before sink ack
	if k.config.EnableAutoCommit {
		err := k.consumer.CommitMessages(ctx, msg)
		if err != nil {
			return errors.Errorf("consumer auto commit message error: %v", err)
		}
	}

	e := k.NewEvent()
	header := e.Header()
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
	k.Send(e)
	return nil
}

func (k *Source) DoCommit(events []api.Event) {
	// commit when sink ack
	if !k.config.EnableAutoCommit {
		var msgs []kafka.Message
		for _, e := range events {
			h := e.Header()
			if _, exist := h["kafka"]; !exist {
				continue
			}

			k, ok := h["kafka"].(map[string]interface{})
			if !ok {
				continue
			}
			if _, exist := k["topic"]; !exist {
				continue
			}
			if _, exist := k["partition"]; !exist {
				continue
			}
			if _, exist := k["offset"]; !exist {
				continue
			}

			msgs = append(msgs, kafka.Message{
				Topic:     k["topic"].(string),
				Partition: k["partition"].(int),
				Offset:    k["offset"].(int64),
			})
		}
		if len(msgs) > 0 {
			err := k.consumer.CommitMessages(context.Background(), msgs...)
			log.Error("consumer manually commit messgage error: %v", err)
		}
	}
}
