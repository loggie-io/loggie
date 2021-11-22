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

package eventbus

import (
	"loggie.io/loggie/pkg/core/api"
	"time"
)

type Event struct {
	Topic       string
	PublishTime time.Time
	Data        interface{}
}

func NewEvent(topic string, data interface{}) Event {
	return Event{
		Topic:       topic,
		PublishTime: time.Now(),
		Data:        data,
	}
}

type Listener interface {
	api.Lifecycle
	Name() string
	Config() interface{}
	Subscribe(event Event)
}

type Subscribe struct {
	listener Listener
	async    bool
	topics   []string
}

type SubscribeOpt func(s *Subscribe)

func NewSubscribe(listener Listener, opts ...SubscribeOpt) *Subscribe {
	s := &Subscribe{
		listener: listener,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func WithAsync(async bool) SubscribeOpt {
	return func(s *Subscribe) {
		s.async = async
	}
}

func WithTopic(topic string) SubscribeOpt {
	return func(s *Subscribe) {
		if s.topics == nil {
			s.topics = make([]string, 0)
		}
		s.topics = append(s.topics, topic)
	}
}

func WithTopics(topics []string) SubscribeOpt {
	return func(s *Subscribe) {
		if s.topics == nil {
			s.topics = make([]string, 0)
		}
		s.topics = append(s.topics, topics...)
	}
}
