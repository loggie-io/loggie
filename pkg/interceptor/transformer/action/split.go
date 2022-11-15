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
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/eventops"
	"github.com/pkg/errors"
	"strings"
)

const (
	SplitName     = "split"
	SplitUsageMsg = "usage: split(key) or split(key, to)"
)

func init() {
	RegisterAction(SplitName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewSplit(args, extra)
	})
}

type Split struct {
	key   string
	to    string
	extra *SplitExtra
}

type SplitExtra struct {
	Separator string   `yaml:"separator,omitempty" validate:"required"`
	Max       int      `yaml:"max,omitempty" default:"-1"`
	Keys      []string `yaml:"keys,omitempty" validate:"required"`
}

func NewSplit(args []string, extra cfg.CommonCfg) (*Split, error) {
	aCount := len(args)
	if aCount != 1 && aCount != 2 {
		return nil, errors.Errorf("invalid args, %s", SplitUsageMsg)
	}
	if aCount == 1 {
		args = append(args, HeaderRoot)
	}

	extraCfg := &SplitExtra{}
	if err := cfg.UnpackFromCommonCfg(extra, extraCfg).Validate().Defaults().Do(); err != nil {
		return nil, err
	}

	return &Split{
		key:   args[0],
		to:    args[1],
		extra: extraCfg,
	}, nil
}

func (r *Split) act(e api.Event) error {
	val := eventops.GetString(e, r.key)
	splitResult := strings.SplitN(val, r.extra.Separator, r.extra.Max)
	keys := r.extra.Keys
	if len(splitResult) != len(keys) {
		log.Debug("split failed event: %s", e.String())
		return errors.Errorf("length of split result: %d unequal to keys: %d", len(splitResult), len(keys))
	}
	if r.to == HeaderRoot {
		for i, r := range splitResult {
			k := keys[i]
			eventops.Set(e, k, r)
		}

	} else {
		obj := make(map[string]interface{})
		for i, r := range splitResult {
			k := keys[i]
			obj[k] = r
		}
		eventops.Set(e, r.to, obj)
	}

	if r.key != r.to {
		eventops.Del(e, r.key)
	}

	return nil
}
