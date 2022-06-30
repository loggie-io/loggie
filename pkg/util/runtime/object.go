/*
Copyright 2021 Loggie Authors

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

package runtime

import (
	"strconv"

	"github.com/pkg/errors"
)

const sep = "."

type Object struct {
	data interface{}
}

func NewObject(obj map[string]interface{}) *Object {
	return &Object{
		data: obj,
	}
}

func (obj *Object) Map() (map[string]interface{}, error) {
	if ret, ok := obj.data.(map[string]interface{}); ok {
		return ret, nil
	}
	return nil, errors.New("type assert to map[string]interface{} failed")
}

func (obj *Object) Get(key string) *Object {
	m, err := obj.Map()
	if err == nil {
		if val, ok := m[key]; ok {
			return &Object{
				data: val,
			}
		}
	}
	return &Object{nil}
}

func (obj *Object) GetPath(query string) *Object {
	paths := GetQueryPaths(query)
	return obj.GetPaths(paths)
}

func (obj *Object) GetPaths(paths []string) *Object {
	o := obj
	for _, p := range paths {
		o = o.Get(p)
	}
	return o
}

func (obj *Object) Set(key string, val interface{}) {
	m, err := obj.Map()
	if err != nil {
		return
	}
	m[key] = val
}

func (obj *Object) SetPath(query string, val interface{}) {
	paths := GetQueryPaths(query)
	obj.SetPaths(paths, val)
}

func (obj *Object) SetPaths(paths []string, val interface{}) {
	if len(paths) == 0 {
		obj.data = val
		return
	}

	if _, ok := (obj.data).(map[string]interface{}); !ok {
		obj.data = make(map[string]interface{})
	}
	curr := obj.data.(map[string]interface{})

	for i := 0; i < len(paths)-1; i++ {
		b := paths[i]
		if _, ok := curr[b]; !ok {
			n := make(map[string]interface{})
			curr[b] = n
			curr = n
			continue
		}

		if _, ok := curr[b].(map[string]interface{}); !ok {
			n := make(map[string]interface{})
			curr[b] = n
		}

		curr = curr[b].(map[string]interface{})
	}

	curr[paths[len(paths)-1]] = val
}

func (obj *Object) Del(key string) {
	m, err := obj.Map()
	if err != nil {
		return
	}
	delete(m, key)
}

func (obj *Object) DelPath(query string) {
	paths := GetQueryPaths(query)
	obj.DelPaths(paths)
}

func (obj *Object) DelPaths(paths []string) {
	if len(paths) == 0 {
		return
	}
	if len(paths) == 1 {
		obj.Del(paths[0])
		return
	}

	prefix := paths[:len(paths)-1]
	fin := paths[len(paths)-1]
	tmp := obj.GetPaths(prefix)
	tmp.Del(fin)
}

func (obj *Object) String() (string, error) {
	if obj.data == nil {
		return "", nil
	}
	if s, ok := (obj.data).(string); ok {
		return s, nil
	}
	return "", errors.New("type assertion to string failed")
}

func (obj *Object) Int64() (int64, error) {
	if obj.data == nil {
		return 0, nil
	}
	if s, ok := (obj.data).(int64); ok {
		return s, nil
	}
	return 0, errors.New("type assertion to int failed")
}

func (obj *Object) Float64() (float64, error) {
	if obj.data == nil {
		return 0, nil
	}
	if s, ok := (obj.data).(float64); ok {
		return s, nil
	}
	return 0, errors.New("type assertion to float64 failed")
}

func (obj *Object) IsNull() bool {
	if obj == nil || obj.data == nil {
		return true
	}
	return false
}

func (obj *Object) Value() interface{} {
	return obj.data
}

func (obj *Object) FlatKeyValue(token string) (map[string]interface{}, error) {
	m, err := obj.Map()
	if err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return m, nil
	}
	dest := make(map[string]interface{})
	flatten(token, "", m, dest)
	return dest, nil
}

func flatten(token string, prefix string, src map[string]interface{}, dest map[string]interface{}) {
	if len(prefix) > 0 {
		prefix += token
	}
	for k, v := range src {
		switch child := v.(type) {
		case map[string]interface{}:
			flatten(token, prefix+k, child, dest)
		case []interface{}:
			for i := 0; i < len(child); i++ {
				dest[prefix+k+token+strconv.Itoa(i)] = child[i]
			}
		default:
			dest[prefix+k] = v
		}
	}
}
