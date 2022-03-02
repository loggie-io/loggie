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
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/context"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus/export/logger"
)

// asyncConsumerSize should always be 1 because concurrency may cause panic
var defaultEventCenter = NewEventCenter(2048, 1)

func Publish(topic string, data interface{}) {
	defaultEventCenter.publish(NewEvent(topic, data))
}

func PublishOrDrop(topic string, data interface{}) {
	defaultEventCenter.publishOrDrop(NewEvent(topic, data))
}

func Registry(listenerName string, listenerFactory ListenerFactory, opts ...SubscribeOpt) {
	RegistrySubscribe(NewSubscribe(listenerName, listenerFactory, opts...))
}

func RegistrySubscribe(subscribe *Subscribe) {
	defaultEventCenter.registry(subscribe)
}

func AfterErrorFunc(errorMsg string) {
	Publish(ErrorTopic, ErrorMetricData{
		ErrorMsg: errorMsg,
	})
}

type EventCenter struct {
	done                   chan struct{}
	name2Subscribe         map[string]*Subscribe
	activeTopic2Subscribes map[string][]*Subscribe
	asyncConsumerSize      int
	eventChan              chan Event
}

func NewEventCenter(bufferSize int64, asyncConsumerSize int) *EventCenter {
	ec := &EventCenter{
		done:                   make(chan struct{}),
		name2Subscribe:         make(map[string]*Subscribe),
		activeTopic2Subscribes: make(map[string][]*Subscribe),
		asyncConsumerSize:      asyncConsumerSize,
		eventChan:              make(chan Event, bufferSize),
	}

	return ec
}

func StartAndRun(config Config) {

	defaultEventCenter.start(config)

	for i := 0; i < defaultEventCenter.asyncConsumerSize; i++ {
		go defaultEventCenter.run()
	}
}

func (ec *EventCenter) Stop() {
	close(ec.done)
}

func (ec *EventCenter) registry(subscribe *Subscribe) {
	if _, ok := ec.name2Subscribe[subscribe.listenerName]; ok {
		log.Panic("listener name(%s) repeat!", subscribe.listenerName)
	}
	ec.name2Subscribe[subscribe.listenerName] = subscribe
}

func (ec *EventCenter) publish(event Event) {
	ec.eventChan <- event
}

func (ec *EventCenter) publishOrDrop(event Event) {
	select {
	case ec.eventChan <- event:
	default:
	}
}

func (ec *EventCenter) start(config Config) {
	logger.Run(config.LoggerConfig)

	for name, conf := range config.ListenerConfigs {
		subscribe, ok := ec.name2Subscribe[name]
		if !ok {
			log.Info("unable to find listener: %s", name)
			continue
		}
		subscribe.listener = subscribe.factory()
		listener := subscribe.listener
		listener.Init(&context.DefaultContext{})

		if conf == nil {
			conf = cfg.NewCommonCfg()
		}
		err := cfg.UnpackDefaultsAndValidate(conf, listener.Config())
		if err != nil {
			log.Panic("unpack listener %s config error: %v", name, err)
		}
		config.ListenerConfigs[name] = conf
		listener.Start()
		log.Info("listener(%s) start", listener.Name())

		ec.activeSubscribe(subscribe)
	}
}

func (ec *EventCenter) activeSubscribe(subscribe *Subscribe) {
	for _, topic := range subscribe.topics {
		if subscribes, ok := ec.activeTopic2Subscribes[topic]; ok {
			subscribes = append(subscribes, subscribe)
		} else {
			var subscribes []*Subscribe
			subscribes = append(subscribes, subscribe)
			ec.activeTopic2Subscribes[topic] = subscribes
		}
	}
}

func (ec *EventCenter) run() {
	for {
		select {
		case <-ec.done:
			return
		case e := <-ec.eventChan:
			topic := e.Topic
			if metas, ok := ec.activeTopic2Subscribes[topic]; ok {
				for _, subscribe := range metas {
					subscribe.listener.Subscribe(e)
				}
			} else {
				log.Debug("topic(%s) has no consumer listener", topic)
			}
		}
	}
}
