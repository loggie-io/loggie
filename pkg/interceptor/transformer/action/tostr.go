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
	"reflect"
	"strconv"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/util/eventops"
	"github.com/pkg/errors"
)

const (
	ToStrName     = "toStr"
	ToStrUsageMsg = "usage: toStr(key) or toStr(key, srcType)"

	typeInteger64 = "int64"
	typeFloat64   = "float64"
)

func init() {
	RegisterAction(ToStrName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewConvertStr(args)
	})
}

type ConvertStr struct {
	key     string
	srcType string
}

func NewConvertStr(args []string) (*ConvertStr, error) {
	if len(args) == 1 {
		return &ConvertStr{
			key:     args[0],
			srcType: "",
		}, nil
	}

	if len(args) == 2 {
		srcType := args[1]
		if srcType != typeBoolean && srcType != typeInteger && srcType != typeFloat && srcType != typeInteger64 && srcType != typeFloat64 {
			return nil, errors.New(fmt.Sprintf("unsupport type: %s, choose %s, %s, %s, %s, %s", srcType, typeBoolean, typeInteger, typeFloat, typeInteger64, typeFloat64))
		}

		return &ConvertStr{
			key:     args[0],
			srcType: args[1],
		}, nil
	}

	return nil, errors.Errorf("invalid args, %s", ToStrUsageMsg)
}

func (s *ConvertStr) act(e api.Event) error {
	srcVal := eventops.Get(e, s.key)
	dstVal, err := convertStr(srcVal, s.srcType)
	if err != nil {
		return errors.Errorf("convert field %s value %v from type %s (actually type is %s) to string error: %v", s.key, srcVal, s.srcType, reflect.TypeOf(srcVal), err)
	}
	eventops.Set(e, s.key, dstVal)
	return nil
}

func convertStr(srcVal interface{}, srcType string) (string, error) {
	if srcType == "" {
		return convertStrWithoutType(srcVal), nil
	}
	switch srcType {
	case typeBoolean:
		parseVal, ok := srcVal.(bool)
		if !ok {
			return "", errors.New("srcVal value is not bool")
		}
		dstVal := strconv.FormatBool(parseVal)
		return dstVal, nil

	case typeInteger:
		parseVal, ok := srcVal.(int)
		if !ok {
			return "", errors.New("srcVal value is not int")
		}
		dstVal := strconv.Itoa(parseVal)
		return dstVal, nil

	case typeFloat:
		parseVal, ok := srcVal.(float32)
		if !ok {
			return "", errors.New("srcVal value is not float")
		}
		dstVal := strconv.FormatFloat(float64(parseVal), 'g', -1, 32)
		return dstVal, nil

	case typeInteger64:
		parseVal, ok := srcVal.(int64)
		if !ok {
			return "", errors.New("srcVal value is not int64")
		}
		dstVal := strconv.FormatInt(parseVal, 10)
		return dstVal, nil

	case typeFloat64:
		parseVal, ok := srcVal.(float64)
		if !ok {
			return "", errors.New("srcVal value is not float64")
		}
		dstVal := strconv.FormatFloat(parseVal, 'g', -1, 64)
		return dstVal, nil
	}

	return "", errors.New("unsupported type")
}

func convertStrWithoutType(srcVal interface{}) string {
	switch v := srcVal.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	rv := reflect.ValueOf(srcVal)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32)
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	}
	return fmt.Sprintf("%v", srcVal)
}
