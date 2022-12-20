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
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/interceptor/logalert/condition"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/util"
)

const Type = "logAlert"
const NoDataKey = "NoDataAlert"

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

	regex       []*regexp.Regexp
	ignoreRegex []*regexp.Regexp
	rules       []advancedRule

	nodataMode bool
	ticker     *time.Ticker
	eventFlag  chan struct{}
	done       chan struct{}

	pipelineName string
}

type advancedRule struct {
	regex     *regexp.Regexp
	matchType string
	groups    []advancedRuleGroup
}

type advancedRuleGroup struct {
	key          string
	operatorFunc condition.OperatorFunc
	target       string
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

	if len(i.config.Ignore) > 0 {
		for _, r := range i.config.Ignore {
			regex := regexp.MustCompile(r)

			i.ignoreRegex = append(i.ignoreRegex, regex)
		}
	}

	if i.config.Advanced.Enable {
		for _, mode := range i.config.Advanced.Mode {
			if mode == ModeRegexp {
				for _, rule := range i.config.Advanced.Rules {
					regex := util.MustCompilePatternWithJavaStyle(rule.Regexp)
					aRule := advancedRule{
						regex:     regex,
						matchType: rule.MatchType,
					}
					groups := make([]advancedRuleGroup, 0)
					for _, group := range rule.Groups {
						groups = append(groups, advancedRuleGroup{
							key:          group.Key,
							operatorFunc: condition.OperatorMap[group.Operator],
							target:       group.Value,
						})
					}
					aRule.groups = groups
					i.rules = append(i.rules, aRule)
				}
			} else if mode == ModeNoData {
				i.nodataMode = true
				i.eventFlag = make(chan struct{})
				i.done = make(chan struct{})
			}
		}

	}

	return nil
}

func (i *Interceptor) Start() error {
	if i.nodataMode {
		i.runTicker()
	}
	if i.config.Template != nil {
		eventbus.PublishOrDrop(eventbus.AlertTempTopic, *i.config.Template)
	}

	return nil
}

func (i *Interceptor) Stop() {
	if i.nodataMode {
		close(i.done)
	}
}

func (i *Interceptor) runTicker() {
	duration := i.config.Advanced.Duration
	go func() {
		i.ticker = time.NewTicker(time.Duration(duration) * time.Second)
		defer i.ticker.Stop()

		for {
			select {
			case <-i.done:
				return
			case <-i.ticker.C:
				log.Info("long time no message!")

				header := make(map[string]interface{})

				var e api.Event
				var meta api.Meta
				meta = event.NewDefaultMeta()

				meta.Set(event.SystemProductTimeKey, time.Now())
				if len(i.pipelineName) > 0 {
					meta.Set(event.SystemPipelineKey, i.pipelineName)
				}

				e = event.NewEvent(header, []byte("long time no message!"))
				e.Header()["reason"] = NoDataKey
				if len(i.config.Additions) > 0 {
					e.Header()["_additions"] = i.config.Additions
				}
				e.Fill(meta, header, e.Body())

				eventbus.PublishOrDrop(eventbus.LogAlertTopic, &e)

				eventbus.PublishOrDrop(eventbus.WebhookTopic, &e)

			case <-i.eventFlag:
				i.ticker.Reset(time.Duration(duration) * time.Second)
			}
		}
	}()

}

func (i *Interceptor) Intercept(invoker source.Invoker, invocation source.Invocation) api.Result {
	if i.nodataMode {
		i.eventFlag <- struct{}{}
	}

	ev := invocation.Event
	if len(i.pipelineName) == 0 {
		value, exist := ev.Meta().Get(event.SystemPipelineKey)
		if exist {
			i.pipelineName = value.(string)
		}
	}

	matched, reason, message := i.match(ev)
	if !matched {
		if !invocation.WebhookEnabled {
			return invoker.Invoke(invocation)
		}
		return result.Drop()
	}
	log.Debug("logAlert matched: %s, %s", message, reason)

	// do fire alert
	ev.Header()["reason"] = reason
	if len(i.config.Additions) > 0 {
		ev.Header()["_additions"] = i.config.Additions
	}

	e := ev.DeepCopy()
	eventbus.PublishOrDrop(eventbus.LogAlertTopic, &e)

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

	// ignore
	if len(i.ignoreRegex) > 0 {
		for _, regex := range i.ignoreRegex {
			if regex.MatchString(target) {
				return false, "", ""
			}
		}
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

	if i.config.Advanced.Enable {
		if i.config.Advanced.MatchType == MatchTypeAll && i.matchAll(target) {
			return true, "matches all rules", target
		}

		if i.config.Advanced.MatchType == MatchTypeAny && i.matchAny(target) {
			return true, "matches some rules", target
		}
	}

	return false, "", ""
}

func (i *Interceptor) matchAll(target string) bool {
	for _, rule := range i.rules {
		match := matchRule(target, rule)
		if !match {
			return false
		}
	}
	return true
}

func (i *Interceptor) matchAny(target string) bool {
	for _, rule := range i.rules {
		match := matchRule(target, rule)
		if match {
			return true
		}
	}
	return false
}

func matchRule(target string, rule advancedRule) bool {
	paramsMap := util.MatchGroupWithRegex(rule.regex, target)
	if rule.matchType == MatchTypeAll {
		for _, group := range rule.groups {
			match, _ := matchAdvRule(paramsMap, group)
			if !match {
				//log.Info("target %s does not match rule %s", target, rule.regex.String())
				return false
			}
		}
		return true
	} else if rule.matchType == MatchTypeAny {
		for _, group := range rule.groups {
			match, _ := matchAdvRule(paramsMap, group)
			if match {
				//log.Info("target %s matches rule %s", target, rule.regex.String())
				return true
			}
		}
		return false
	}
	return false
}

func matchAdvRule(paramsMap map[string]string, rule advancedRuleGroup) (bool, error) {
	s, ok := paramsMap[rule.key]
	if ok {
		return rule.operatorFunc(s, rule.target)
	} else {
		return false, errors.New(fmt.Sprintf("body does not contain key %s", rule.key))
	}
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
