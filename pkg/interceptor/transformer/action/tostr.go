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
	"fmt"
	"reflect"
	"strconv"
)

const (
	ToStrName     = "toStr"
	ToStrUsageMsg = "usage: toStr(key)"
)

func init() {
	RegisterAction(ToStrName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewConvertStr(args)
	})
}

type ConvertStr struct {
	key string
}

func NewConvertStr(args []string) (*ConvertStr, error) {
	if len(args) != 1 {
		return nil, errors.Errorf("invalid args, %s", ToStrUsageMsg)
	}

	return &ConvertStr{
		key: args[0],
	}, nil
}

func (s *ConvertStr) act(e api.Event) error {
	srcVal := eventops.Get(e, s.key)
	dstVal := convertStr(srcVal)
	eventops.Set(e, s.key, dstVal)
	return nil
}

func convertStr(srcVal interface{}) string {
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
