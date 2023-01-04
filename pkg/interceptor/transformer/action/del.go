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
	DelName     = "del"
	DelUsageMsg = "usage: del(key1, key2...)"
)

func init() {
	RegisterAction(DelName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewDel(args)
	})
}

type Del struct {
	key []string
}

func NewDel(args []string) (*Del, error) {
	if len(args) == 0 {
		return nil, errors.Errorf("invalid args, %s", DelUsageMsg)
	}

	return &Del{
		key: args,
	}, nil
}

func (d *Del) act(e api.Event) error {
	eventops.DelKeys(e, d.key)
	return nil
}

func (d *Del) start() error { return nil }

func (d *Del) stop() {}
