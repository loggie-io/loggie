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

package rocketmq

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/pkg/errors"
)

const Type = "rocketmq"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

// Sink ...
type Sink struct {
	config *Config
	clt    *rocketmqClient
	cod    codec.Codec

	topicPattern *pattern.Pattern
	tagPattern   *pattern.Pattern
}

type rocketmqClient struct {
	rocketmqClient rocketmq.Producer
	options        []producer.Option
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

func (s *Sink) Init(context api.Context) error {
	s.topicPattern, _ = pattern.Init(s.config.Topic)
	s.tagPattern, _ = pattern.Init(s.config.Tag)
	return nil
}

func (s *Sink) Start() error {
	c := s.config
	log.Debug("init rocketmq client config")
	options, err := getOptions(c)
	if err != nil {
		return err
	}
	client, err := newRocketmqClient(options)
	if err != nil {
		return errors.WithMessagef(err, "new rocketmq client failed")
	}
	if client == nil {
		return errors.New("client nil")
	}
	// set rocketmq logger
	rlog.SetLogger(newLoggieLogWrapper())
	// start producer
	err = client.rocketmqClient.Start()
	s.clt = client

	return err
}

func newRocketmqClient(
	options []producer.Option,
) (*rocketmqClient, error) {
	client, err := rocketmq.NewProducer(options...)
	if err == nil {
		return &rocketmqClient{
			rocketmqClient: client,
			options:        options,
		}, nil
	}

	return nil, err
}

func (s *Sink) String() string {
	return fmt.Sprintf("%s/%s", api.SINK, Type)
}

func (s *Sink) Stop() {
	if s.clt != nil {
		_ = s.clt.rocketmqClient.Shutdown()
	}
}

func (s *Sink) Consume(batch api.Batch) api.Result {
	events := batch.Events()
	var msgs []*primitive.Message
	for _, event := range events {
		msg := &primitive.Message{}
		// try to dynamically render topic
		topic, err := s.selectTopic(event)
		if err != nil {
			failedConfig := s.config.IfRenderTopicFailed
			if !failedConfig.IgnoreError {
				log.Error("render rocketmq topic error: %v; event is: %s", err, event.String())
			}
			if failedConfig.DefaultValue != "" { // if we had a default topic, set topic to this one
				topic = failedConfig.DefaultValue
			} else if failedConfig.DropEvent {
				// ignore(drop) this event in default
				continue
			} else {
				return result.Fail(errors.WithMessage(err, "render rocketmq topic error"))
			}
		}
		msg.Topic = topic
		// try to dynamically render tag, the tag is not required.
		// for RocketMQ, dynamically rendering tag may be more meaningful than dynamically rendering topic.
		if s.config.Tag != "" {
			var tag string
			if tag, err = s.selectTag(event); err != nil {
				failedConfig := s.config.IfRenderTagFailed
				if !failedConfig.IgnoreError {
					log.Error("render rocketmq tag error: %v; event is: %s", err, event.String())
				}
				if failedConfig.DefaultValue != "" { // if we had a default tag, set tag to this one
					tag = failedConfig.DefaultValue
				} else if failedConfig.DropEvent {
					// ignore(drop) this event in default
					continue
				} else {
					return result.Fail(errors.WithMessage(err, "render rocketmq tag error"))
				}
			}
			if tag != "" {
				msg.WithTag(tag)
			}
		}
		// encode event to message body
		if serializerEncode, err := s.cod.Encode(event); err != nil {
			log.Warn("encode event error: %+v", err)
			return result.Fail(err)
		} else {
			msg.Body = serializerEncode
		}
		// set message keys
		if len(s.config.MessageKeys) > 0 {
			msg.WithKeys(s.config.MessageKeys)
		}
		msgs = append(msgs, msg)
	}
	// send batch messages to rocketmq async
	if _, sendErr := s.clt.rocketmqClient.SendSync(context.Background(), msgs...); sendErr != nil {
		return result.Fail(sendErr)
	}
	return result.Success()
}

func (s *Sink) selectTopic(e api.Event) (string, error) {
	return s.topicPattern.WithObject(runtime.NewObject(e.Header())).RenderWithStrict()
}

func (s *Sink) selectTag(e api.Event) (string, error) {
	return s.tagPattern.WithObject(runtime.NewObject(e.Header())).RenderWithStrict()
}
