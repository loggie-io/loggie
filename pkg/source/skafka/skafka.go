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
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

const Type = "skafka"

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
	consumer  sarama.ConsumerGroup
	handler   ConsumerHandler
	eventPool *event.Pool
	cancel    context.CancelFunc
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
	cfg := sarama.NewConfig()
	var err error
	cfg.Version, err = sarama.ParseKafkaVersion(k.config.Version)
	if err != nil {
		return errors.WithMessage(err, "parse kafka version err")
	}
	cfg.Consumer.Group.Session.Timeout = time.Second * 60
	cfg.Net.ReadTimeout = cfg.Consumer.Group.Session.Timeout + 30*time.Second
	cfg.Consumer.Group.Heartbeat.Interval = time.Second * 10
	cfg.Consumer.MaxProcessingTime = time.Second * 10

	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.AutoCommit.Enable = k.config.EnableAutoCommit
	cfg.Consumer.Offsets.AutoCommit.Interval = k.config.AutoCommitInterval
	cfg.Consumer.Offsets.Initial = getAutoOffset(k.config.AutoOffsetReset)
	hostname, _ := os.Hostname()
	cfg.ClientID = hostname

	if len(k.config.SASL.UserName) > 0 && len(k.config.SASL.Password) > 0 {
		cfg.Net.SASL.Enable = true
		if len(k.config.SASL.Type) > 0 {
			cfg.Net.SASL.Mechanism = sarama.SASLMechanism(strings.ToUpper(k.config.SASL.Type))
		}
		//cfg.Net.SASL.Mechanism = sarama.SASLMechanism(k.config.SASL.Type)
		cfg.Net.SASL.User = k.config.SASL.UserName
		cfg.Net.SASL.Password = k.config.SASL.Password
	}
	err = cfg.Validate()
	if err != nil {
		return errors.WithMessage(err, "sarama validate err")
	}
	//cfg.Version = sarama.V0_10_2_0
	k.consumer, err = sarama.NewConsumerGroup(k.config.Brokers, k.config.GroupId, cfg)
	if err != nil {
		return errors.WithMessage(err, "new kafka consumer err")
	}
	k.handler = ConsumerHandler{
		ready:           make(chan bool),
		ReadBufferChan:  make(chan *sessMsg, 2048),
		highWaterOffset: make(map[int32]int64),
		currentOffset:   make(map[int32]int64),
		lock:            new(sync.RWMutex),
	}
	go k.init()
	return nil
}
func (k *Source) init() {
	ctx, cancel := context.WithCancel(context.Background())
	k.cancel = cancel
	go func() {
		for {
			err := k.consumer.Consume(context.Background(), strings.Split(k.config.Topic, ","), &k.handler)
			if err != nil {
				log.Error("%+v", err)
			}
			if ctx.Err() != nil {
				return
			}
			k.handler.ready = make(chan bool)
		}
	}()
	<-k.handler.ready
}
func (k *Source) Stop() {
	k.closeOnce.Do(func() {
		if k.consumer != nil {
			err := k.consumer.Close()
			if err != nil {
				log.Error("close kafka consumer error: %+v", err)
			}
		}
		k.cancel()
		close(k.done)
	})
}

func (k *Source) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", k.String())

	if k.consumer == nil {
		log.Error("kakfa consumer not initialized yet")
		return
	}

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

	msg := <-k.handler.ReadBufferChan
	e := k.eventPool.Get()
	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}
	header["kafka"] = map[string]interface{}{
		"offset":    msg.msg.Offset,
		"partition": msg.msg.Partition,
		"timestamp": msg.msg.Timestamp.Format(time.RFC3339),
		"topic":     msg.msg.Topic,
	}

	for _, h := range msg.msg.Headers {
		header[string(h.Key)] = string(h.Value)
	}
	e.Fill(e.Meta(), header, msg.msg.Value)
	productFunc(e)
	msg.sess.MarkMessage(msg.msg, "")
	return nil
}

func (k *Source) Commit(events []api.Event) {
	// commit when sink ack
	k.eventPool.PutAll(events)
}
