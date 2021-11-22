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

package logalert

import (
	"fmt"
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/core/sink"
	"loggie.io/loggie/pkg/core/sysconfig"
	"loggie.io/loggie/pkg/eventbus"
	"loggie.io/loggie/pkg/pipeline"
	"regexp"
	"strings"
)

const Type = "logAlert"

func init() {
	pipeline.Register(api.INTERCEPTOR, Type, makeInterceptor)
}

func makeInterceptor(info pipeline.Info) api.Component {
	return &Interceptor{
		config: &Config{},
	}
}

type Interceptor struct {
	config *Config
}

func (i *Interceptor) Config() interface{} {
	return i.config
}

func (i *Interceptor) Category() api.Category {
	return api.INTERCEPTOR
}

func (i *Interceptor) Type() api.Type {
	return Type
}

func (i *Interceptor) String() string {
	return fmt.Sprintf("%s/%s", i.Category(), i.Type())
}

func (i *Interceptor) Init(context api.Context) {
}

func (i *Interceptor) Start() {
}

func (i *Interceptor) Stop() {
}

func (i *Interceptor) Intercept(invoker sink.Invoker, invocation sink.Invocation) api.Result {

	events := invocation.Batch.Events()
	for _, ev := range events {
		matched, reason, message := i.match(ev)
		if !matched {
			continue
		}
		log.Debug("logAlert matched: %s, %s", message, reason)

		// do fire alert
		labels := map[string]string{
			"host":   sysconfig.NodeName,
			"source": ev.Source(),
		}
		annotations := map[string]string{
			"reason":  reason,
			"message": message,
		}

		if len(i.config.Labels.FromHeader) != 0 {
			for _, key := range i.config.Labels.FromHeader {
				val, ok := ev.Header()[key]
				if !ok {
					continue
				}
				valStr, ok := val.(string)
				if !ok {
					continue
				}
				labels[key] = valStr
			}
		}

		d := eventbus.NewLogAlertData(labels, annotations)
		eventbus.PublishOrDrop(eventbus.LogAlertTopic, d)
	}

	return invoker.Invoke(invocation)
}

func (i *Interceptor) match(event api.Event) (matched bool, reason string, message string) {
	matcher := i.config.Matcher

	var target string
	if i.config.Matcher.TargetHeader != "" {
		header, ok := event.Header()[i.config.Matcher.TargetHeader].(string)
		if !ok {
			return false, "", ""
		}
		target = header
	} else {
		target = string(event.Body())
	}

	// containers matcher
	if len(matcher.Contains) != 0 {
		for _, substr := range matcher.Contains {
			if strings.Contains(target, substr) {
				return true, fmt.Sprintf("contained %s", substr), target
			}
		}
	}

	// regexp matcher
	if len(matcher.Regexp) != 0 {
		for _, reg := range matcher.Regexp {
			matched, err := regexp.MatchString(reg, target)
			if err != nil {
				log.Warn("regexp %s match error: %v", reg, err)
				continue
			}
			if matched {
				return true, fmt.Sprintf("matched %s", reg), target
			}
		}
	}

	return false, "", ""
}
