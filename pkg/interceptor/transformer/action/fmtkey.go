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
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/pkg/errors"
	"regexp"
)

const (
	FmtKeyName     = "fmtKey"
	FmtKeyUsageMsg = "usage: fmtKey()"
)

func init() {
	RegisterAction(FmtKeyName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewFmtKey(args, extra)
	})
}

type FmtKey struct {
	reg   *regexp.Regexp
	extra *FmtKeyExtra
}

type FmtKeyExtra struct {
	Regex   string `yaml:"regex,omitempty" validate:"required"`
	Replace string `yaml:"replace,omitempty" validate:"required"`
}

func NewFmtKey(args []string, extra cfg.CommonCfg) (*FmtKey, error) {
	aCount := len(args)
	if aCount != 0 {
		return nil, errors.Errorf("invalid args, %s", FmtKeyUsageMsg)
	}

	extraCfg := &FmtKeyExtra{}
	if err := cfg.UnpackFromCommonCfg(extra, extraCfg).Do(); err != nil {
		return nil, err
	}
	pattern, err := extraCfg.compile()
	if err != nil {
		return nil, err
	}

	return &FmtKey{
		reg:   pattern,
		extra: extraCfg,
	}, nil
}

func (re *FmtKeyExtra) compile() (*regexp.Regexp, error) {
	if re.Regex == "" {
		return nil, errors.New("fmtKey pattern is required")
	}

	p, err := util.CompilePatternWithJavaStyle(re.Regex)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (r *FmtKey) act(e api.Event) error {
	header := e.Header()
	if header == nil {
		return nil
	}

	obj := runtime.NewObject(header)
	return obj.ConvertKeys(func(key string) string {
		return r.matchAndReplace(key)
	})
}

func (r *FmtKey) matchAndReplace(key string) string {
	matched := r.reg.FindStringSubmatch(key)
	if len(matched) > 0 {
		return r.reg.ReplaceAllString(key, r.extra.Replace)
	}
	return ""
}
