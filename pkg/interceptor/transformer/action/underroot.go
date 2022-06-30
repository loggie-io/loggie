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
	UnderRootName     = "underRoot"
	UnderRootUsageMsg = "usage: underRoot(key)"
)

func init() {
	RegisterAction(UnderRootName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewUnderRoot(args)
	})
}

type UnderRoot struct {
	key string
}

func NewUnderRoot(args []string) (*UnderRoot, error) {
	if len(args) != 1 {
		return nil, errors.Errorf("invalid args, %s", UnderRootUsageMsg)
	}

	return &UnderRoot{
		key: args[0],
	}, nil
}

func (d *UnderRoot) act(e api.Event) error {
	eventops.UnderRoot(e, d.key)
	return nil
}
