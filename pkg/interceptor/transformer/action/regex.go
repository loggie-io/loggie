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

package action

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/loggie-io/loggie/pkg/util/eventops"
	"github.com/pkg/errors"
	"regexp"
)

const (
	RegexName     = "regex"
	RegexUsageMsg = "usage: regex(key) or regex(key, to)"
)

func init() {
	RegisterAction(RegexName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewRegex(args, extra)
	})
}

type Regex struct {
	key   string
	to    string
	reg   *regexp.Regexp
	extra *regexExtra
}

type regexExtra struct {
	Pattern string `yaml:"pattern,omitempty"`
}

func (re *regexExtra) compile() (*regexp.Regexp, error) {
	if re.Pattern == "" {
		return nil, errors.New("regex pattern is required")
	}

	p, err := util.CompilePatternWithJavaStyle(re.Pattern)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func NewRegex(args []string, extra cfg.CommonCfg) (*Regex, error) {
	aCount := len(args)
	if aCount != 1 && aCount != 2 {
		return nil, errors.Errorf("invalid args, %s", RegexUsageMsg)
	}
	if aCount == 1 {
		args = append(args, HeaderRoot)
	}

	extraCfg := &regexExtra{}
	if err := cfg.Unpack(extra, extraCfg); err != nil {
		return nil, err
	}

	pattern, err := extraCfg.compile()
	if err != nil {
		return nil, err
	}

	return &Regex{
		key:   args[0],
		to:    args[1],
		reg:   pattern,
		extra: extraCfg,
	}, nil
}

func (r *Regex) act(e api.Event) error {
	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	// get field value
	val := eventops.GetString(e, r.key)

	if r.to == HeaderRoot {
		matchCount := util.MatchGroupWithRegexAndHeader(r.reg, val, header)
		if matchCount == 0 {
			return errors.Errorf("match group with regex %s is empty", r.extra.Pattern)
		}

	} else {
		matchedMap := util.MatchGroupWithRegex(r.reg, val)
		eventops.Set(e, r.key, matchedMap)
	}

	if r.key != r.to {
		eventops.Del(e, r.key)
	}

	return nil
}
