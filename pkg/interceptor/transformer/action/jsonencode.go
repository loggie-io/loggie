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
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/util/eventops"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/pkg/errors"
)

const (
	JsonEncodeName     = "jsonEncode"
	JsonEncodeUsageMsg = "usage: jsonEncode(key) or jsonEncode(key, to)"
)

func init() {
	RegisterAction(JsonEncodeName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewJsonEncode(args)
	})
}

type JsonEncode struct {
	key string
	to  string
}

func NewJsonEncode(args []string) (*JsonEncode, error) {
	aCount := len(args)
	if aCount != 1 && aCount != 2 {
		return nil, errors.Errorf("invalid args, %s", JsonEncodeUsageMsg)
	}

	// encode json to origin key in default
	if aCount == 1 {
		args = append(args, args[0])
	}

	if args[0] == event.Body {
		return nil, errors.New("body unsupported")
	}
	if args[1] == HeaderRoot {
		args[1] = args[0]
	}

	return &JsonEncode{
		key: args[0],
		to:  args[1],
	}, nil
}

func (j *JsonEncode) act(e api.Event) error {
	obj := runtime.NewObject(e.Header())

	fieldVal, err := obj.GetPath(j.key).Map()
	if err != nil {
		return err
	}

	rawVal, err := json.MarshalToString(fieldVal)
	if err != nil {
		return err
	}

	eventops.Set(e, j.to, rawVal)

	// delete origin fields
	if j.key != j.to {
		eventops.Del(e, j.key)
	}

	return nil
}
