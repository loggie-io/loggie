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

package eventops

import (
	"strconv"
)

type Number struct {
	data float64
}

func NewNumber(n interface{}) (*Number, error) {
	f, err := castToFloat(n)
	if err != nil {
		return nil, err
	}
	return &Number{
		data: f,
	}, nil
}

func castToFloat(v interface{}) (float64, error) {
	switch vv := v.(type) {
	case int:
		return float64(vv), nil
	case int8:
		return float64(vv), nil
	case int16:
		return float64(vv), nil
	case int32:
		return float64(vv), nil
	case int64:
		return float64(vv), nil
	case uint:
		return float64(vv), nil
	case uint8:
		return float64(vv), nil
	case uint16:
		return float64(vv), nil
	case uint32:
		return float64(vv), nil
	case uint64:
		return float64(vv), nil
	case float32:
		return float64(vv), nil
	case float64:
		return vv, nil
	case string:
		// if error occurred, return zero value
		f, err := strconv.ParseFloat(vv, 64)
		if err != nil {
			return 0, err
		}
		return f, nil
	}
	return 0, nil
}

func (n *Number) Equal(target *Number) bool {
	return n.data == target.data
}

func (n *Number) Greater(target *Number) bool {
	return n.data > target.data
}

func (n *Number) Less(target *Number) bool {
	return n.data < target.data
}
