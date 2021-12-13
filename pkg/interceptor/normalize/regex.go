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
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/event"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/util"
	"regexp"
)

const ProcessorRegex = "regex"

type RegexProcessor struct {
	config *RegexConfig

	regex *regexp.Regexp
}

type RegexConfig struct {
	Target    string `yaml:"target,omitempty" default:"body"`
	Pattern   string `yaml:"pattern,omitempty"`
	UnderRoot bool   `yaml:"underRoot,omitempty" default:"true"`
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

func (r *RegexProcessor) Init() {
	log.Info("regex pattern: %s", r.config.Pattern)
	r.regex = util.CompilePatternWithJavaStyle(r.config.Pattern)
}

func (r *RegexProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	paramsMap := make(map[string]string)
	if r.config.Target == event.Body {
		paramsMap = util.MatchGroupWithRegex(r.regex, string(e.Body()))
	} else {
		targetHeader, ok := header[r.config.Target]
		if ok {
			log.Info("cannot find target fields %s", r.config.Target)
			log.Debug("regex failed event: %s", e.String())
			return nil
		}
		targetHeaderStr, ok := targetHeader.(string)
		if !ok {
			log.Info("target %s value is not string in event %s", r.config.Target)
			log.Debug("regex failed event: %s", e.String())
			return nil
		}

		paramsMap = util.MatchGroupWithRegex(r.regex, targetHeaderStr)
	}

	pl := len(paramsMap)
	if pl == 0 {
		log.Info("match group with regex %s is empty", r.regex.String())
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
