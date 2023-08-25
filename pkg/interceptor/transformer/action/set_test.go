/*
Copyright 2023 Loggie Authors

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
	"testing"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/stretchr/testify/assert"
)

func TestStrConvertSet_Act(t *testing.T) {
	assertions := assert.New(t)

	type fields struct {
		key     string
		value   string
		dstType string
	}
	type args struct {
		e api.Event
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   api.Event
	}{
		{
			name: "set bool",
			fields: fields{
				key:     "a.b",
				value:   "true",
				dstType: typeBoolean,
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": true,
				},
			}, []byte("this is body")),
		},
		{
			name: "set int",
			fields: fields{
				key:     "a.b",
				value:   "200",
				dstType: typeInteger,
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": int64(200),
				},
			}, []byte("this is body")),
		},
		{
			name: "set float",
			fields: fields{
				key:     "a.b",
				value:   "200",
				dstType: typeFloat,
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": float64(200),
				},
			}, []byte("this is body")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StrConvSet{
				key:     tt.fields.key,
				value:   tt.fields.value,
				dstType: tt.fields.dstType,
			}
			err := s.act(tt.args.e)
			assertions.NoError(err)
			assertions.Equal(tt.want, tt.args.e)
		})
	}
}
