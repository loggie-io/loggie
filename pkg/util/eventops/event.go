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
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/runtime"
)

func Get(e api.Event, key string) interface{} {
	// find body first
	if key == event.Body && len(e.Body()) != 0 {
		return e.Body()
	}

	obj := runtime.NewObject(e.Header())
	fieldVal := obj.GetPath(key)
	if fieldVal.Value() == nil {
		return nil
	}

	return fieldVal.Value()
}

func GetString(e api.Event, key string) string {
	// find body first
	if key == event.Body && len(e.Body()) != 0 {
		return string(e.Body()) // TODO bytes to string
	}

	// then get from header
	obj := runtime.NewObject(e.Header())
	fieldVal := obj.GetPath(key)

	strVal, err := fieldVal.String()
	if err != nil {
		log.Warn("fields %s value %v is not string, err: %v", key, fieldVal, err)
		return ""
	}

	return strVal
}

func GetBytes(e api.Event, key string) []byte {
	// find body first
	if key == event.Body && len(e.Body()) != 0 {
		return e.Body()
	}

	obj := runtime.NewObject(e.Header())
	fieldVal := obj.GetPath(key)

	strVal, err := fieldVal.String()
	if err != nil {
		log.Warn("fields %s value %v is not string, err: %v", key, fieldVal, err)
		return []byte{}
	}

	return []byte(strVal)
}

func GetNumber(e api.Event, key string) (*Number, error) {
	obj := runtime.NewObject(e.Header())
	fieldVal := obj.GetPath(key)

	n, err := NewNumber(fieldVal.Value())
	if err != nil {
		return nil, err
	}

	return n, nil
}

func Set(e api.Event, key string, val interface{}) {
	obj := runtime.NewObject(e.Header())

	obj.SetPath(key, val)
}

func Copy(e api.Event, from string, to string) {
	obj := runtime.NewObject(e.Header())

	if from == event.Body && len(e.Body()) != 0 {
		obj.SetPath(to, e.Body())
		return
	}

	fieldVal := obj.GetPath(from)
	obj.SetPath(to, fieldVal.Value())
}

func Del(e api.Event, key string) {
	obj := runtime.NewObject(e.Header())

	if key == event.Body {
		e.Fill(e.Meta(), e.Header(), []byte{})
		return

	} else {
		obj.DelPath(key)
	}
}

func DelKeys(e api.Event, keys []string) {
	obj := runtime.NewObject(e.Header())

	for _, key := range keys {
		if key == event.Body {
			e.Fill(e.Meta(), e.Header(), []byte{})
			return

		} else {
			obj.DelPath(key)
		}
	}
}

func Move(e api.Event, from string, to string) {
	obj := runtime.NewObject(e.Header())

	if from == event.Body && len(e.Body()) != 0 {
		obj.SetPath(to, string(e.Body()))
		e.Fill(e.Meta(), e.Header(), []byte{})
		return
	}

	fieldVal := obj.GetPath(from)
	obj.DelPath(from)

	// set value to new fields
	obj.SetPath(to, fieldVal.Value())
}

func UnderRoot(e api.Event, key string) {
	obj := runtime.NewObject(e.Header())

	upperPaths, key := runtime.GetQueryUpperPaths(key)
	upperVal := obj.GetPaths(upperPaths)
	val := upperVal.Get(key)
	if valMap, err := val.Map(); err != nil {
		obj.Set(key, val.Value())
	} else {
		for k, v := range valMap {
			obj.Set(k, v)
		}
	}

	upperVal.Del(key)
}
