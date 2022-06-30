/*
Copyright 2022 Loggie Authors

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

package transformer

import (
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/interceptor/transformer/action"
	"github.com/loggie-io/loggie/pkg/interceptor/transformer/condition"
)

type Config struct {
	interceptor.ExtensionConfig `yaml:",inline"`
	Actions                     []StepConfig `yaml:"actions"`
	IgnoreError                 bool         `yaml:"ignoreError,omitempty"`
}

func (c *Config) Validate() error {
	for _, act := range c.Actions {
		if err := act.Validate(); err != nil {
			return err
		}
	}

	if _, err := NewActions(c.Actions); err != nil {
		return err
	}

	return nil
}

type StepConfig struct {
	ConditionConfig condition.Config `yaml:",inline,omitempty"` // if-then-else
	ActionConfig    action.Config    `yaml:",inline,omitempty"` // only actions
}

// withCondition indicate action config contains `if-then-else` workflow
func (c *StepConfig) withCondition() bool {
	if c.ConditionConfig.If != "" {
		return true
	}
	return false
}

func (c *StepConfig) Validate() error {
	if c.withCondition() {
		if err := c.ConditionConfig.Validate(); err != nil {
			return err
		}
	} else {
		if err := c.ActionConfig.Validate(); err != nil {
			return err
		}
	}

	return nil
}
