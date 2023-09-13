/*
Copyright 2023 Loggie Authors

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

package action

import (
	"regexp"
	"strings"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/util/eventops"
	"github.com/pkg/errors"
)

const (
	ReplaceName     = "replace"
	ReplaceUsageMsg = "usage: replace(key)"

	ReplaceRegexName     = "replaceRegex"
	ReplaceRegexUsageMsg = "usage: replaceRegex(key)"
)

func init() {
	RegisterAction(ReplaceName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewReplace(args, extra)
	})

	RegisterAction(ReplaceRegexName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewReplaceRegex(args, extra)
	})
}

type Replace struct {
	key   string
	extra *ReplaceExtra
}

type ReplaceExtra struct {
	Old string `yaml:"old,omitempty" validate:"required"`
	New string `yaml:"new,omitempty" validate:"required"`
	Max int    `yaml:"max,omitempty" default:"-1"`
}

func NewReplace(args []string, extra cfg.CommonCfg) (*Replace, error) {
	aCount := len(args)
	if aCount != 1 {
		return nil, errors.Errorf("invalid args, %s", ReplaceRegexUsageMsg)
	}

	extraCfg := &ReplaceExtra{}
	if err := cfg.UnpackFromCommonCfg(extra, extraCfg).Validate().Defaults().Do(); err != nil {
		return nil, err
	}

	if extraCfg.Max == 0 {
		extraCfg.Max = -1
	}

	return &Replace{
		key:   args[0],
		extra: extraCfg,
	}, nil
}

func (r *Replace) act(e api.Event) error {
	val := eventops.GetString(e, r.key)
	replaceResult := strings.Replace(val, r.extra.Old, r.extra.New, r.extra.Max)
	eventops.Set(e, r.key, replaceResult)
	return nil
}

type ReplaceRegex struct {
	key   string
	reg   *regexp.Regexp
	extra *ReplaceRegexExtra
}

type ReplaceRegexExtra struct {
	Expression string `yaml:"expression,omitempty" validate:"required"`
	Replace    string `yaml:"replace,omitempty" validate:"required"`
}

func NewReplaceRegex(args []string, extra cfg.CommonCfg) (*ReplaceRegex, error) {
	aCount := len(args)
	if aCount != 1 {
		return nil, errors.Errorf("invalid args, %s", ReplaceUsageMsg)
	}

	extraCfg := &ReplaceRegexExtra{}
	if err := cfg.UnpackFromCommonCfg(extra, extraCfg).Validate().Defaults().Do(); err != nil {
		return nil, err
	}

	expr, err := extraCfg.compile()
	if err != nil {
		return nil, err
	}

	return &ReplaceRegex{
		key:   args[0],
		reg:   expr,
		extra: extraCfg,
	}, nil
}

func (r *ReplaceRegex) act(e api.Event) error {
	val := eventops.GetString(e, r.key)
	match := r.reg.ReplaceAllString(val, r.extra.Replace)
	eventops.Set(e, r.key, match)
	return nil
}

func (r *ReplaceRegexExtra) compile() (*regexp.Regexp, error) {
	if r.Expression == "" {
		return nil, errors.New("regex expression is required")
	}

	expr, err := regexp.Compile(r.Expression)
	if err != nil {
		return nil, err
	}

	return expr, nil
}
