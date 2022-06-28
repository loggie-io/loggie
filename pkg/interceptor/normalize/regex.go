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

package normalize

import (
	"regexp"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/loggie-io/loggie/pkg/util/runtime"
)

const ProcessorRegex = "regex"

type RegexProcessor struct {
	config      *RegexConfig
	interceptor *Interceptor
	regex       *regexp.Regexp
}

type RegexConfig struct {
	Target      string `yaml:"target,omitempty" default:"body"`
	Pattern     string `yaml:"pattern,omitempty" validate:"required"`
	UnderRoot   bool   `yaml:"underRoot,omitempty" default:"true"`
	IgnoreError bool   `yaml:"ignoreError"`
}

func init() {
	register(ProcessorRegex, func() Processor {
		return NewRegexProcessor()
	})
}

func NewRegexProcessor() *RegexProcessor {
	return &RegexProcessor{
		config: &RegexConfig{},
	}
}

func (r *RegexProcessor) Config() interface{} {
	return r.config
}

func (r *RegexProcessor) GetName() string {
	return ProcessorRegex
}

func (r *RegexProcessor) Init(interceptor *Interceptor) {
	log.Info("regex pattern: %s", r.config.Pattern)
	r.interceptor = interceptor
	r.regex = util.MustCompilePatternWithJavaStyle(r.config.Pattern)
}

func (r *RegexProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	var paramsMap map[string]string
	if r.config.Target == event.Body {
		paramsMap = util.MatchGroupWithRegex(r.regex, string(e.Body()))
	} else {
		obj := runtime.NewObject(header)
		targetVal, err := obj.GetPath(r.config.Target).String()
		if err != nil {
			LogErrorWithIgnore(r.config.IgnoreError, "get target %s failed: %v", r.config.Target, err)
			log.Debug("regex failed event: %s", e.String())
			r.interceptor.reportMetric(r)
			return nil
		}
		if targetVal == "" {
			log.Debug("target %s value is empty, event is: %s", r.config.Target, e.String())
			return nil
		}

		paramsMap = util.MatchGroupWithRegex(r.regex, targetVal)
	}

	pl := len(paramsMap)
	if pl == 0 {
		LogErrorWithIgnore(r.config.IgnoreError, "match group with regex %s is empty", r.regex.String())
		log.Debug("regex failed event: %s", e.String())
		return nil
	}

	if r.config.UnderRoot {
		for k, v := range paramsMap {
			header[k] = v
		}
	} else {
		header[SystemLogBody] = paramsMap
	}

	return nil
}
