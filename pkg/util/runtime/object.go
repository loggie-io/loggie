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
	"sync"

	"github.com/barkimedes/go-deepcopy"
	"github.com/pkg/errors"
)

type Object struct {
	mu   *sync.RWMutex
	data interface{}
}

func NewObject(obj any) *Object {
	return &Object{
		data: obj,
		mu:   &sync.RWMutex{},
	}
}

type safeMap struct {
	mu   *sync.Mutex
	data map[string]interface{}
}

func (s *safeMap) Value() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data
}

func (s *safeMap) Copy() (map[string]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ret, err := deepcopy.Anything(s.data)
	if err != nil {
		return nil, err
	}
	if r, ok := ret.(map[string]interface{}); ok {
		return r, nil
	}
	return nil, errors.New("type assert to map[string]interface{} failed")
}

func (s *safeMap) Set(key string, val interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = val
}

func (obj *Object) Map() (map[string]interface{}, error) {
	obj.mu.RLock()
	defer obj.mu.RUnlock()
	if ret, ok := obj.data.(map[string]interface{}); ok {
		return ret, nil
	}
	return nil, errors.New("type assert to map[string]interface{} failed")
}
func (obj *Object) Copy() (*Object, error) {
	obj.mu.Lock()
	defer obj.mu.Unlock()
	ret, err := deepcopy.Anything(obj.data)
	if err != nil {
		return nil, err
	}
	return NewObject(ret), nil
}

func (obj *Object) Get(key string) *Object {
	m, err := obj.Map()
	if err == nil {
		obj.mu.RLock()
		defer obj.mu.RUnlock()
		if val, ok := m[key]; ok {
			return &Object{
				data: val,
				mu:   obj.mu,
			}
		}
	}
	return &Object{data: nil, mu: obj.mu}
}

func (obj *Object) GetPath(query string) *Object {
	obj.mu.RLock()
	defer obj.mu.RUnlock()
	paths := GetQueryPaths(query)
	return obj.GetPaths(paths)
}

func (obj *Object) GetPaths(paths []string) *Object {
	obj.mu.RLock()
	o := obj
	obj.mu.RUnlock()
	for _, p := range paths {
		m, err := o.Map()
		if err != nil {
			return &Object{data: nil, mu: obj.mu}
		}
		obj.mu.RLock()
		val, ok := m[p]
		obj.mu.RUnlock()
		if !ok {
			return &Object{data: nil, mu: obj.mu}
		}
		o = &Object{data: val, mu: obj.mu}
	}
	return o
}

func (obj *Object) Set(key string, val interface{}) {
	obj.mu.Lock()
	defer obj.mu.Unlock()
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
	obj.mu.Lock() // 加写锁保护整个修改过程
	defer obj.mu.Unlock()

	if len(paths) == 0 {
		obj.data = val
		return
	}

	// 确保当前数据是 map 类型
	if _, ok := obj.data.(map[string]interface{}); !ok {
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
		// 若当前值不是 map，强制转为 map
		if childMap, ok := curr[b].(map[string]interface{}); ok {
			curr = childMap
		} else {
			n := make(map[string]interface{})
			curr[b] = n
			curr = n
		}
	}

	curr[paths[len(paths)-1]] = val
}

func (obj *Object) Del(key string) {
	m, err := obj.Map()
	if err != nil {
		return
	}
	obj.mu.Lock()
	defer obj.mu.Unlock()
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
		obj.Del(paths[0]) // 直接调用 Del（内部会加写锁）
		return
	}

	// 先获取子路径的对象（仅用读锁）
	obj.mu.RLock()
	prefix := paths[:len(paths)-1]
	fin := paths[len(paths)-1]
	obj.mu.RUnlock() // 提前释放读锁，避免后续写锁冲突
	tmp := obj.GetPaths(prefix)

	// 对临时对象执行删除（此时无读锁，可安全获取写锁）
	tmp.Del(fin)
}

func (obj *Object) String() (string, error) {
	obj.mu.RLock()
	defer obj.mu.RUnlock()
	if obj.data == nil {
		return "", nil
	}
	if s, ok := (obj.data).(string); ok {
		return s, nil
	}
	return "", errors.New("type assertion to string failed")
}

func (obj *Object) Int64() (int64, error) {
	obj.mu.RLock()
	defer obj.mu.RUnlock()
	if obj.data == nil {
		return 0, nil
	}
	if s, ok := (obj.data).(int64); ok {
		return s, nil
	}
	return 0, errors.New("type assertion to int failed")
}

func (obj *Object) Float64() (float64, error) {
	obj.mu.RLock()
	defer obj.mu.RUnlock()
	if obj.data == nil {
		return 0, nil
	}
	if s, ok := (obj.data).(float64); ok {
		return s, nil
	}
	return 0, errors.New("type assertion to float64 failed")
}

func (obj *Object) IsNull() bool {
	obj.mu.RLock()
	defer obj.mu.RUnlock()
	if obj == nil || obj.data == nil {
		return true
	}
	return false
}

func (obj *Object) Value() interface{} {
	obj.mu.RLock()
	defer obj.mu.RUnlock()
	return obj.data
}

func (obj *Object) FlatKeyValue(token string) (map[string]interface{}, error) {
	o, err := obj.Copy()
	if err != nil {
		return nil, err
	}
	m, err := o.Map()
	if err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return m, nil
	}
	obj.mu.Lock()
	dest := &safeMap{
		mu:   &sync.Mutex{},
		data: make(map[string]interface{}),
	}
	flatten(token, "", m, dest)
	obj.mu.Unlock()
	return dest.Copy()
}

func flatten(token string, prefix string, src map[string]interface{}, dest *safeMap) {
	if len(prefix) > 0 {
		prefix += token
	}
	for k, v := range src {
		switch child := v.(type) {
		case map[string]interface{}:
			flatten(token, prefix+k, child, dest)
		case []interface{}:
			for i := 0; i < len(child); i++ {
				dest.Set(prefix+k+token+strconv.Itoa(i), child[i])
			}
		default:
			dest.Set(prefix+k, v)
		}
	}
}

type convertKeyFunc func(key string) string

// ConvertKeys All keys in the Object are processed by the convertKeyFunc function.
// If convertKeyFunc returns empty, the key will not be processed.
func (obj *Object) ConvertKeys(keyFunc convertKeyFunc) error {
	m, err := obj.Map()
	if err != nil {
		return err
	}
	if len(m) == 0 {
		return nil
	}
	obj.mu.Lock()
	convertKeys(m, keyFunc)
	obj.mu.Unlock()
	return nil
}

func convertKeys(src map[string]interface{}, keyFunc convertKeyFunc) {
	for k, v := range src {
		switch child := v.(type) {
		case map[string]interface{}:
			convertKeys(child, keyFunc)

		default:
			out := keyFunc(k)
			if out != "" {
				src[out] = v
				delete(src, k)
			}
		}
	}
}
