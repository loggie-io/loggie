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
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var data = map[string]interface{}{
	"a": "b",
	"c": 1,
	"d": map[string]interface{}{
		"e": "f",
		"g": 2,
	},
}

func TestObject_Get(t *testing.T) {
	type fields struct {
		data interface{}
	}
	type args struct {
		key string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Object
	}{
		{
			name: "ok-string",
			fields: fields{
				data: data,
			},
			args: args{
				key: "a",
			},
			want: &Object{
				data: "b",
			},
		},
		{
			name: "ok-map",
			fields: fields{
				data: data,
			},
			args: args{
				key: "d",
			},
			want: &Object{
				data: map[string]interface{}{
					"e": "f",
					"g": 2,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := NewObject(tt.fields.data)
			got := obj.Get(tt.args.key)
			assert.Equal(t, tt.want.data, got.data)
		})
	}
}

func TestObject_GetPaths(t *testing.T) {
	type fields struct {
		data interface{}
	}
	type args struct {
		paths []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Object
	}{
		{
			name: "ok",
			fields: fields{
				data: data,
			},
			args: args{
				paths: []string{"d", "e"},
			},
			want: &Object{
				data: "f",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := NewObject(tt.fields.data)
			if got := obj.GetPaths(tt.args.paths); !reflect.DeepEqual(got.data, tt.want.data) {
				t.Errorf("GetPaths() = %v, want %v", got.data, tt.want.data)
			}
		})
	}
}

func TestObject_DelPaths(t *testing.T) {
	type fields struct {
		data interface{}
	}
	type args struct {
		paths []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Object
	}{
		{
			name: "ok",
			fields: fields{
				data: data,
			},
			args: args{
				paths: []string{"d", "e"},
			},
			want: &Object{
				data: map[string]interface{}{
					"a": "b",
					"c": 1,
					"d": map[string]interface{}{
						"g": 2,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := NewObject(tt.fields.data)
			obj.DelPaths(tt.args.paths)

			if !reflect.DeepEqual(obj.data, tt.want.data) {
				t.Errorf("DelPaths() = %v, want %v", obj.data, tt.want.data)
			}

		})
	}
}

func TestObject_SetPaths(t *testing.T) {
	type fields struct {
		data interface{}
	}
	type args struct {
		paths []string
		val   interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Object
	}{
		{
			name: "ok",
			fields: fields{
				data: map[string]interface{}{
					"a": "b",
					"d": map[string]interface{}{
						"e": "f",
						"g": 2,
					},
				},
			},
			args: args{
				paths: []string{"d", "h"},
				val:   "k",
			},
			want: &Object{
				data: map[string]interface{}{
					"a": "b",
					"d": map[string]interface{}{
						"e": "f",
						"g": 2,
						"h": "k",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := NewObject(tt.fields.data)

			obj.SetPaths(tt.args.paths, tt.args.val)
			if !reflect.DeepEqual(obj.data, tt.want.data) {
				t.Errorf("SetPaths() = %v, want %v", obj.data, tt.want.data)
			}
		})
	}
}

func TestObject_GetPath(t *testing.T) {
	type fields struct {
		data interface{}
	}
	type args struct {
		query string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Object
	}{
		{
			name: "ok-sin",
			fields: fields{
				data: map[string]interface{}{
					"a": "b",
					"c": 1,
					"d": map[string]interface{}{
						"e": "f",
						"g": 2,
					},
				},
			},
			args: args{
				query: "c",
			},
			want: &Object{
				data: 1,
			},
		},
		{
			name: "ok-paths",
			fields: fields{
				data: map[string]interface{}{
					"a": "b",
					"c": 1,
					"d": map[string]interface{}{
						"e": "f",
						"g": 2,
					},
				},
			},
			args: args{
				query: "d.e",
			},
			want: &Object{
				data: "f",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := NewObject(tt.fields.data)

			if got := obj.GetPath(tt.args.query); !reflect.DeepEqual(got.data, tt.want.data) {
				t.Errorf("GetPath() = %v, want %v", got.data, tt.want.data)
			}
		})
	}
}

func TestObject_FlatKeyValue(t *testing.T) {
	type fields struct {
		data interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]interface{}
	}{
		{
			name: "ok",
			fields: fields{
				data: map[string]interface{}{
					"a": "f",
					"b": map[string]interface{}{
						"c": "g",
					},
				},
			},
			want: map[string]interface{}{
				"a":   "f",
				"b_c": "g",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := NewObject(tt.fields.data)
			got, err := obj.FlatKeyValue("_")
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestObject_FlattenConcurrent(t *testing.T) {
	t.Skip("此测试故意触发崩溃，默认跳过")
	dest := &safeMap{
		mu:   &sync.Mutex{},
		data: make(map[string]interface{}),
	}
	src := map[string]interface{}{
		"a": 1,
		"b": map[string]interface{}{"c": 2},
		"d": []interface{}{3, 4},
	}

	var wg sync.WaitGroup

	// 启动一个 goroutine 持续修改 dest（调用  写入）
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500000; i++ { // 多次写入触发冲突
			time.Sleep(time.Nanosecond)
			flatten(".", "", src, dest)
		}
	}()

	// 启动另一个 goroutine 持续迭代 dest（触发并发迭代）
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500000; i++ { // 多次迭代触发冲突
			ret, _ := dest.Copy()
			for range ret { // 仅迭代键，不关心值
				time.Sleep(time.Nanosecond) // 增加冲突概率
			}
			for range src { // 仅迭代键，不关心值
				time.Sleep(time.Nanosecond) // 增加冲突概率
			}
		}
	}()

	wg.Wait()
	// 如果未崩溃，说明测试未触发冲突（概率性），可增加循环次数
	t.Error("未触发并发 map 错误（可能是概率问题）")
}

func TestObject_FlatKeyValueConcurrent(t *testing.T) {
	t.Skip("此测试故意触发崩溃，默认跳过")
	var dest map[string]interface{}
	src := map[string]interface{}{
		"a": 1,
		"b": map[string]interface{}{"c": 2},
		"d": []interface{}{3, 4},
	}
	obj := NewObject(src)
	var wg sync.WaitGroup

	// 启动一个 goroutine 持续修改 dest（调用  写入）
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000000; i++ { // 多次写入触发冲突
			time.Sleep(time.Nanosecond)
			dest, _ = obj.FlatKeyValue("token")
		}
	}()

	// 启动另一个 goroutine 持续迭代 dest（触发并发迭代）
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000000; i++ { // 多次迭代触发冲突
			for range dest { // 仅迭代键，不关心值
				time.Sleep(time.Nanosecond) // 增加冲突概率
			}
			for range src { // 仅迭代键，不关心值
				time.Sleep(time.Nanosecond) // 增加冲突概率
			}
			_, _ = obj.Map()
			_ = obj.Get("a")
			_, _ = obj.String()
			obj.GetPaths([]string{"a", "b", "c"})
			obj.SetPath("c", map[string]interface{}{"c": 2})
			obj.DelPaths([]string{"a", "b", "c"})
		}
	}()

	wg.Wait()
	// 如果未崩溃，说明测试未触发冲突（概率性），可增加循环次数
	t.Error("未触发并发 map 错误（可能是概率问题）")
}

func TestObject_ConvertKeys(t *testing.T) {
	type fields struct {
		data interface{}
	}
	type args struct {
		keyFunc convertKeyFunc
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Object
	}{
		{
			name: "update keys ok",
			fields: fields{
				data: map[string]interface{}{
					"a": "b",
					"c": "d",
					"e": map[string]interface{}{
						"f": "g",
					},
				},
			},
			args: args{
				keyFunc: func(key string) string {
					if key == "a" {
						return "aa"
					}
					if key == "f" {
						return "ff"
					}
					return ""
				},
			},
			want: &Object{
				data: map[string]interface{}{
					"aa": "b",
					"c":  "d",
					"e": map[string]interface{}{
						"ff": "g",
					},
				},
			},
		},
		{
			name: "regex keys ok",
			fields: fields{
				data: map[string]interface{}{
					"a": "b",
					"e": map[string]interface{}{
						"foo.bar/test": "g",
					},
				},
			},
			args: args{
				keyFunc: func(key string) string {
					reg := regexp.MustCompile("foo.bar/(.*)")
					matched := reg.FindStringSubmatch(key)
					if len(matched) > 0 {
						return reg.ReplaceAllString(key, "pre-${1}")
					}
					return ""
				},
			},
			want: &Object{
				data: map[string]interface{}{
					"a": "b",
					"e": map[string]interface{}{
						"pre-test": "g",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := NewObject(tt.fields.data)
			err := obj.ConvertKeys(tt.args.keyFunc)
			assert.NoError(t, err)
			assert.Equal(t, tt.want.data, obj.data)
		})
	}
}
