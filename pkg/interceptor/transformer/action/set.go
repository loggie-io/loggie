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
	SetName     = "set"
	SetUsageMsg = "usage: set(key, value)"

	SetFloatName = "setFloat"
	SetFloatMsg  = "usage: setFloat(key, value)"

	SetIntName = "setInt"
	SetIntMsg  = "usage: setInt(key, value)"

	SetBoolName = "setBool"
	SetBoolMsg  = "usage: setBool(key, value)"
)

func init() {
	RegisterAction(SetName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewSet(args)
	})
	RegisterAction(SetFloatName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewStrConvSet(args, "float", SetFloatMsg)
	})
	RegisterAction(SetIntName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewStrConvSet(args, "int", SetIntMsg)
	})
	RegisterAction(SetBoolName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewStrConvSet(args, "bool", SetBoolMsg)
	})
}

// Set is same with Add
type Set struct {
	key   string
	value string
}

func NewSet(args []string) (*Set, error) {
	if len(args) != 2 {
		return nil, errors.Errorf("invalid args, %s", SetUsageMsg)
	}

	return &Set{
		key:   args[0],
		value: args[1],
	}, nil
}

func (a *Set) act(e api.Event) error {
	eventops.Set(e, a.key, a.value)
	return nil
}

type StrConvSet struct {
	key     string
	value   string
	dstType string
}

func NewStrConvSet(args []string, destType, msg string) (*StrConvSet, error) {
	if len(args) != 2 {
		return nil, errors.Errorf("invalid args, %s", msg)
	}

	return &StrConvSet{
		key:     args[0],
		value:   args[1],
		dstType: destType,
	}, nil
}

func (a *StrConvSet) act(e api.Event) error {
	dstVal, err := convert(a.value, a.dstType)
	if err != nil {
		return errors.Errorf("convert field %s value %v to type %s error: %v", a.key, a.value, a.dstType, err)
	}

	eventops.Set(e, a.key, dstVal)
	return nil
}
