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
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/util/eventops"
	"github.com/pkg/errors"
	"strconv"
)

const (
	StrConvertName     = "strconv"
	StrConvertUsageMsg = "usage: strconv(key, type)"

	typeBoolean = "bool"
	typeInteger = "int"
	typeFloat   = "float"
)

func init() {
	RegisterAction(StrConvertName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewStrConvert(args)
	})
}

type StrConvert struct {
	key     string
	dstType string
}

func NewStrConvert(args []string) (*StrConvert, error) {
	if len(args) != 2 {
		return nil, errors.Errorf("invalid args, %s, type: %s, %s, %s", StrConvertUsageMsg, typeBoolean, typeInteger, typeFloat)
	}

	dstType := args[1]
	if dstType != typeBoolean && dstType != typeInteger && dstType != typeFloat {
		return nil, errors.New(fmt.Sprintf("unsupport type: %s, choose %s, %s %s", dstType, typeBoolean, typeInteger, typeFloat))
	}

	return &StrConvert{
		key:     args[0],
		dstType: args[1],
	}, nil
}

func (s *StrConvert) act(e api.Event) error {
	srcVal := eventops.GetString(e, s.key)

	dstVal, err := convert(srcVal, s.dstType)
	if err != nil {
		return errors.Errorf("convert field %s value %v to type %s error: %v", s.key, srcVal, s.dstType, err)
	}

	eventops.Set(e, s.key, dstVal)
	return nil
}

func convert(srcVal, dstType string) (interface{}, error) {
	switch dstType {
	case typeBoolean:
		dstVal, err := strconv.ParseBool(srcVal)
		if err != nil {
			return nil, err
		}
		return dstVal, nil

	case typeInteger:
		dstVal, err := strconv.ParseInt(srcVal, 10, 64)
		if err != nil {
			return nil, err
		}
		return dstVal, nil

	case typeFloat:
		dstVal, err := strconv.ParseFloat(srcVal, 64)
		if err != nil {
			return nil, err
		}
		return dstVal, nil
	}

	return nil, errors.New("unsupported type")
}
