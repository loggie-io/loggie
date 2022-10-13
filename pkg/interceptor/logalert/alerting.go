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
	"regexp"
	"strings"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/global"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/pipeline"
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

	regex []*regexp.Regexp
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

func (i *Interceptor) Init(context api.Context) error {

	if len(i.config.Matcher.Regexp) != 0 {
		for _, r := range i.config.Matcher.Regexp {
			regex := regexp.MustCompile(r) // after validated

			i.regex = append(i.regex, regex)
		}
	}
	return nil
}

func (i *Interceptor) Start() error {
	return nil
}

func (i *Interceptor) Stop() {
}

func (i *Interceptor) Intercept(invoker source.Invoker, invocation source.Invocation) api.Result {

	ev := invocation.Event
	matched, reason, message := i.match(ev)
	if !matched {
		return invoker.Invoke(invocation)
	}
	log.Debug("logAlert matched: %s, %s", message, reason)

	// do fire alert
	labels := map[string]string{
		"host":   global.NodeName,
		"source": ev.Meta().Source(),
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

	if len(i.regex) != 0 {
		for _, reg := range i.regex {
			matched := reg.MatchString(target)
			if matched {
				return true, fmt.Sprintf("matched %s", reg), target
			}
		}
	}

	return false, "", ""
}

func (i *Interceptor) Order() int {
	return i.config.Order
}

func (i *Interceptor) BelongTo() (componentTypes []string) {
	return i.config.BelongTo
}

func (i *Interceptor) IgnoreRetry() bool {
	return true
}
