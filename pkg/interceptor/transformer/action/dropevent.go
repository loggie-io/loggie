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
	"errors"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/cfg"
)

const (
	DropEventName     = "dropEvent"
	DropEventUsageMsg = "usage: dropEvent()"
)

var ErrorDropEvent = errors.New("DROP_EVENT")

func init() {
	RegisterAction(DropEventName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewDropEvent(args)
	})
}

type DropEvent struct {
}

func NewDropEvent(args []string) (*DropEvent, error) {
	if len(args) != 0 {
		return nil, errors.New(fmt.Sprintf("invalid args, %s", DropEventUsageMsg))
	}

	return &DropEvent{}, nil
}

func (a *DropEvent) act(e api.Event) error {
	return ErrorDropEvent
}

func (a *DropEvent) start() error { return nil }

func (a *DropEvent) stop() {}
