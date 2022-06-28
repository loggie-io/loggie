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
	jsoniter "github.com/json-iterator/go"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/util/eventops"
	"github.com/pkg/errors"
)

const (
	JsonDecodeName     = "jsonDecode"
	JsonDecodeUsageMsg = "usage: jsonDecode(key) or jsonDecode(key, to)"
)

var (
	json = jsoniter.ConfigFastest
)

func init() {
	RegisterAction(JsonDecodeName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewJsonDecode(args)
	})
}

type JsonDecode struct {
	key string
	to  string
}

func NewJsonDecode(args []string) (*JsonDecode, error) {
	aCount := len(args)
	if aCount != 1 && aCount != 2 {
		return nil, errors.Errorf("invalid args, %s", JsonDecodeUsageMsg)
	}

	// decode json to header root in default
	if aCount == 1 {
		args = append(args, HeaderRoot)
	}

	return &JsonDecode{
		key: args[0],
		to:  args[1],
	}, nil
}

func (j *JsonDecode) act(e api.Event) error {
	// get raw json data
	rawVal := eventops.GetBytes(e, j.key)

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	if j.to == HeaderRoot {
		// under root
		if err := json.Unmarshal(rawVal, &header); err != nil {
			return err
		}

	} else {
		tmp := make(map[string]interface{})
		if err := json.Unmarshal(rawVal, &tmp); err != nil {
			return err
		}
		eventops.Set(e, j.to, tmp)
	}

	// delete origin fields
	if j.key != j.to {
		eventops.Del(e, j.key)
	}

	return nil
}
