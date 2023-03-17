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

package skafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	slog "log"
	"os"
	"time"
)

const Type = "skafka"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Sink struct {
	config *Config
	writer sarama.AsyncProducer
	cod    codec.Codec
}

func NewSink() *Sink {
	return &Sink{
		config: &Config{},
	}
}

func (s *Sink) Config() interface{} {
	return s.config
}

func (s *Sink) Category() api.Category {
	return api.SINK
}

func (s *Sink) SetCodec(c codec.Codec) {
	s.cod = c
}

func (s *Sink) Type() api.Type {
	return Type
}

func (s *Sink) String() string {
	return fmt.Sprintf("%s/%s", api.SINK, Type)
}

func (s *Sink) Init(context api.Context) error {
	return nil
}

func (s *Sink) Start() error {
	c := s.config
	cfg := sarama.NewConfig()
	cfg.ClientID, _ = os.Hostname()
	sarama.Logger = slog.New(os.Stdout, "[Sarama] ", slog.LstdFlags)
	cfg.Producer.Partitioner = func(topic string) sarama.Partitioner { return sarama.NewRoundRobinPartitioner(topic) }
	if len(c.SASL.UserName) > 0 && len(c.SASL.Password) > 0 {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = c.SASL.UserName
		cfg.Net.SASL.Password = c.SASL.Password
	}
	cfg.Producer.Compression = compression(c.Compression)
	var err error
	cfg.Version, err = sarama.ParseKafkaVersion(c.Version)
	if err != nil {
		return err
	}
	cfg.Net.DialTimeout = 30 * time.Second
	w, err := sarama.NewAsyncProducer(c.Brokers, cfg)
	if err != nil {
		return err
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
	//km := make([]*sarama.ProducerMessage, 0, l)
	for _, e := range events {
		value, err := s.cod.Encode(e)
		if err != nil {
			log.Warn("encode event error: %+v", err)
			return result.Fail(err)
		}
		msg := &sarama.ProducerMessage{
			Topic: s.config.Topic,
			Value: sarama.StringEncoder(value),
		}

		s.writer.Input() <- msg
		//km = append(km, message)
	}
	return result.Success()
	//if len(km) == 0 {
	//	return result.DropWith(errors.New("send to kafka message batch is null"))
	//}
	//
	//if s.writer != nil {
	//	err := s.writer.WriteMessages(context.Background(), km...)
	//	if err != nil {
	//		return result.Fail(errors.WithMessage(err, "write to kafka"))
	//	}
	//
	//	return result.Success()
	//}
	//
	//return result.Fail(errors.New("kafka sink writer not initialized"))
}
