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
	CopyName     = "copy"
	CopyUsageMsg = "usage: copy(from, to)"
)

func init() {
	RegisterAction(CopyName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewCopy(args)
	})
}

type Copy struct {
	from string
	to   string
}

func NewCopy(args []string) (*Copy, error) {
	if len(args) != 2 {
		return nil, errors.Errorf("invalid args, %s", CopyUsageMsg)
	}

	return &Copy{
		from: args[0],
		to:   args[1],
	}, nil
}

func (c *Copy) act(e api.Event) error {
	eventops.Copy(e, c.from, c.to)
	return nil
}
