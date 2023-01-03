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

	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/interceptor/logalert/condition"
	"github.com/loggie-io/loggie/pkg/util"
)

const (
	ModeRegexp   = "regexp"
	ModeNoData   = "noData"
	MatchTypeAll = "all"
	MatchTypeAny = "any"
)

type Config struct {
	interceptor.ExtensionConfig `yaml:",inline"`

	Matcher         Matcher           `yaml:"matcher,omitempty"`
	Labels          Labels            `yaml:"labels,omitempty"`
	Additions       map[string]string `yaml:"additions,omitempty"`
	Ignore          []string          `yaml:"ignore,omitempty"`
	Advanced        Advanced          `yaml:"advanced,omitempty"`
	Template        *string           `yaml:"template,omitempty"`
	SendOnlyMatched bool              `yaml:"sendOnlyMatched,omitempty"`
}

type Matcher struct {
	Regexp       []string `yaml:"regexp,omitempty"`
	Contains     []string `yaml:"contains,omitempty"`
	TargetHeader string   `yaml:"target,omitempty"`
}

type Labels struct {
	FromHeader []string `yaml:"from,omitempty"`
}

type Advanced struct {
	Enable    bool     `yaml:"enable"`
	Mode      []string `yaml:"mode,omitempty"`
	Duration  int      `yaml:"duration,omitempty"`
	MatchType string   `yaml:"matchType,omitempty"`
	Rules     []Rule   `yaml:"rules,omitempty"`
}

type Rule struct {
	Regexp    string  `yaml:"regexp,omitempty"`
	MatchType string  `yaml:"matchType,omitempty"`
	Groups    []Group `yaml:"groups,omitempty"`
}

type Group struct {
	Key      string `yaml:"key,omitempty"`
	Operator string `yaml:"operator,omitempty"`
	Value    string `yaml:"value,omitempty"`
}

func (c *Config) Validate() error {
	if len(c.Matcher.Regexp) != 0 {
		for _, reg := range c.Matcher.Regexp {
			_, err := regexp.Compile(reg)
			if err != nil {
				return err
			}
		}
	}

	if len(c.Ignore) > 0 {
		for _, reg := range c.Ignore {
			_, err := regexp.Compile(reg)
			if err != nil {
				return err
			}
		}
	}

	if c.Advanced.Enable {
		err := c.Advanced.validate()
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *Advanced) validate() error {
	for _, mode := range a.Mode {
		if mode == ModeNoData {
			if err := a.validateInNodataMode(); err != nil {
				return err
			}
		} else if mode == ModeRegexp {
			if err := a.validateInRegexpMode(); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("advanced logAlert mode %s not supported", a.Mode)
		}
	}
	return nil
}

func (a *Advanced) validateInNodataMode() error {
	if a.Duration == 0 {
		return errors.New("advanced logAlert no data duration should > 0")
	}
	return nil
}

func (a *Advanced) validateInRegexpMode() error {
	if a.MatchType == MatchTypeAny || a.MatchType == MatchTypeAll {
		if len(a.Rules) == 0 {
			return errors.New("advanced logAlert has no rules")
		}
		for _, rule := range a.Rules {
			err := rule.validate()
			if err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("advanced logAlert match type %s not supported", a.MatchType)
	}

	return nil
}

func (r *Rule) validate() error {
	_, err := util.CompilePatternWithJavaStyle(r.Regexp)
	if err != nil {
		return err
	}

	if r.MatchType != MatchTypeAny && r.MatchType != MatchTypeAll {
		return fmt.Errorf("advanced logAlert match type %s not supported", r.MatchType)
	}

	if len(r.Groups) == 0 {
		return errors.New("rule has no group")
	}

	for _, group := range r.Groups {
		if _, ok := condition.OperatorMap[group.Operator]; !ok {
			return fmt.Errorf("operator %s not supported", group.Operator)
		}
	}

	return nil
}
