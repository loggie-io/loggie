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
	"loggie.io/loggie/pkg/core/cfg"
	"loggie.io/loggie/pkg/core/context"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/eventbus/export/logger"
)

// asyncConsumerSize should always be 1 because concurrency may cause panic
var defaultEventCenter = NewEventCenter(2048, 1)

func Publish(topic string, data interface{}) {
	defaultEventCenter.publish(NewEvent(topic, data))
}

func PublishOrDrop(topic string, data interface{}) {
	defaultEventCenter.publishOrDrop(NewEvent(topic, data))
}

func Registry(listener Listener, opts ...SubscribeOpt) {
	RegistrySubscribe(NewSubscribe(listener, opts...))
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
	done               chan struct{}
	subscribes         map[string][]*Subscribe
	listeners          map[string]Listener
	asyncConsumerSize  int
	asyncConsumerCount int32
	eventChan          chan Event
}

func NewEventCenter(bufferSize int64, asyncConsumerSize int) *EventCenter {
	ec := &EventCenter{
		done:              make(chan struct{}),
		subscribes:        make(map[string][]*Subscribe),
		listeners:         make(map[string]Listener),
		asyncConsumerSize: asyncConsumerSize,
		eventChan:         make(chan Event, bufferSize),
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
	ec.listeners[subscribe.listener.Name()] = subscribe.listener

	subscribe.listener.Init(&context.DefaultContext{})

	for _, topic := range subscribe.topics {
		if subscribes, ok := ec.subscribes[topic]; ok {
			subscribes = append(subscribes, subscribe)
		} else {
			subscribes := make([]*Subscribe, 0)
			subscribes = append(subscribes, subscribe)
			ec.subscribes[topic] = subscribes
		}
	}
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
		listener, ok := ec.listeners[name]
		if !ok {
			log.Info("unable to find listener: %s", name)
			continue
		}

		if conf == nil {
			conf = cfg.NewCommonCfg()
		}
		err := cfg.UnpackDefaultsAndValidate(conf, listener.Config())
		if err != nil {
			log.Panic("unpack listener %s config error: %v", name, err)
		}
		config.ListenerConfigs[name] = conf
		listener.Start()
	}
}

func (ec *EventCenter) run() {
	for {
		select {
		case <-ec.done:
			return
		case e := <-ec.eventChan:
			topic := e.Topic
			if metas, ok := ec.subscribes[topic]; ok {
				for _, subscribe := range metas {
					subscribe.listener.Subscribe(e)
				}
			} else {
				log.Debug("topic(%s) has no consumer listener", topic)
			}
		}
	}
}
