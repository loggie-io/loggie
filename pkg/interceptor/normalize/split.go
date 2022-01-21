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
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"strings"
)

const ProcessorSplit = "split"

type SplitProcessor struct {
	config *SplitConfig
}

type SplitConfig struct {
	Target    string   `yaml:"target,omitempty" default:"body"`
	Separator string   `yaml:"separator,omitempty" validate:"required"`
	Max       int      `yaml:"max,omitempty" default:"-1"`
	Keys      []string `yaml:"keys,omitempty"`
}

func init() {
	register(ProcessorSplit, func() Processor {
		return NewSplitProcessor()
	})
}

func NewSplitProcessor() *SplitProcessor {
	return &SplitProcessor{
		config: &SplitConfig{},
	}
}

func (r *SplitProcessor) Config() interface{} {
	return r.config
}

func (r *SplitProcessor) Init() {
}

func (r *SplitProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	target := r.config.Target
	obj := runtime.NewObject(header)

	var val string
	if target == event.Body {
		val = string(e.Body())
	} else {
		t, err := obj.GetPath(target).String()
		if err != nil {
			log.Info("target %s is not string", target)
			log.Debug("split failed event: %s", e.String())
			return nil
		}
		if t == "" {
			log.Info("cannot find target fields %s", target)
			log.Debug("split failed event: %s", e.String())
			return nil
		}
		val = t
	}

	splitResult := strings.SplitN(val, r.config.Separator, r.config.Max)
	keys := r.config.Keys
	if len(splitResult) != len(keys) {
		log.Info("length of split result: %d unequal to keys: %d", len(splitResult), len(keys))
		log.Debug("split failed event: %s", e.String())
		return nil
	}
	for i, r := range splitResult {
		k := keys[i]
		obj.SetPath(k, r)
	}

	return nil
}
