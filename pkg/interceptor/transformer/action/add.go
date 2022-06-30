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
	"github.com/loggie-io/loggie/pkg/util/eventops"
	"github.com/pkg/errors"
)

const (
	AddName     = "add"
	AddUsageMsg = "usage: add(key, value)"
)

func init() {
	RegisterAction(AddName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewAdd(args)
	})
}

type Add struct {
	key   string
	value string
}

func NewAdd(args []string) (*Add, error) {
	if len(args) != 2 {
		return nil, errors.Errorf("invalid args, %s", AddUsageMsg)
	}

	return &Add{
		key:   args[0],
		value: args[1],
	}, nil
}

func (a *Add) act(e api.Event) error {
	eventops.Set(e, a.key, a.value)
	return nil
}
