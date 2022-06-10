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

	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/pkg/errors"

	"github.com/segmentio/kafka-go"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
)

const Type = "kafka"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Sink struct {
	config *Config
	writer *kafka.Writer
	cod    codec.Codec

	topicPattern *pattern.Pattern
}

func NewSink() *Sink {
	return &Sink{
		config: &Config{},
	}
}

func (s *Sink) Config() interface{} {
	return s.config
}

func (s *Sink) SetCodec(c codec.Codec) {
	s.cod = c
}

func (s *Sink) Category() api.Category {
	return api.SINK
}

func (s *Sink) Type() api.Type {
	return Type
}

func (s *Sink) String() string {
	return fmt.Sprintf("%s/%s", api.SINK, Type)
}

func (s *Sink) Init(context api.Context) error {
	s.topicPattern, _ = pattern.Init(s.config.Topic)
	return nil
}

func (s *Sink) Start() error {
	c := s.config
	mechanism, err := mechanism(c.SASL.Type, c.SASL.UserName, c.SASL.Password, c.SASL.Algorithm)
	if err != nil {
		log.Error("kafka sink sasl mechanism with error: %s", err.Error())
		return err
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(c.Brokers...),
		MaxAttempts:  c.MaxAttempts,
		Balancer:     balanceInstance(c.Balance),
		BatchSize:    c.BatchSize,
		BatchBytes:   c.BatchBytes,
		BatchTimeout: c.BatchTimeout,
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(c.RequiredAcks),
		Compression:  compression(c.Compression),
		Transport: &kafka.Transport{
			SASL: mechanism,
		},
	}

	log.Info("kafka-sink start,topic: %s,broker: %v", s.config.Topic, s.config.Brokers)
	s.writer = w
	return nil
}

func (s *Sink) Stop() {
	if s.writer != nil {
		_ = s.writer.Close()
	}
}

func (s *Sink) Consume(batch api.Batch) api.Result {
	events := batch.Events()
	l := len(events)
	if l == 0 {
		return nil
	}
	km := make([]kafka.Message, 0, l)
	for _, e := range events {
		topic, err := s.selectTopic(e)
		if err != nil {
			log.Error("select kafka topic error: %+v", err)
			return result.Fail(err)
		}

		msg, err := s.cod.Encode(e)
		if err != nil {
			log.Warn("encode event error: %+v", err)
			return result.Fail(err)
		}

		km = append(km, kafka.Message{
			Value: msg,
			Topic: topic,
		})
	}

	if s.writer != nil {
		err := s.writer.WriteMessages(context.Background(), km...)
		if err != nil {
			return result.Fail(errors.WithMessage(err, "write to kafka"))
		}

		return result.Success()
	}

	return result.Fail(errors.New("kafka sink writer not initialized"))
}

func (s *Sink) selectTopic(e api.Event) (string, error) {
	return s.topicPattern.WithObject(runtime.NewObject(e.Header())).Render()
}
