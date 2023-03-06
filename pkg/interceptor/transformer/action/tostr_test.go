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
	"testing"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/stretchr/testify/assert"
)

func Test1ConvertStr_Act(t *testing.T) {
	assertions := assert.New(t)
	type fields struct {
		key     string
		srcType string
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
			name: "parse bool without srcType",
			fields: fields{
				key:     "a.b",
				srcType: "",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": true,
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "true",
				},
			}, []byte("this is body")),
		},
		{
			name: "parse bool with srcType",
			fields: fields{
				key:     "a.b",
				srcType: "bool",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": true,
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "true",
				},
			}, []byte("this is body")),
		},
		{
			name: "parse int without srcType",
			fields: fields{
				key:     "a.b",
				srcType: "",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": 200,
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "200",
				},
			}, []byte("this is body")),
		},
		{
			name: "parse int with srcType",
			fields: fields{
				key:     "a.b",
				srcType: "int",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": 200,
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "200",
				},
			}, []byte("this is body")),
		},
		{
			name: "parse float without srcType",
			fields: fields{
				key:     "a.b",
				srcType: "",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": 200.1,
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "200.1",
				},
			}, []byte("this is body")),
		},
		{
			name: "parse float with srcType",
			fields: fields{
				key:     "a.b",
				srcType: "float",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": float32(200.1),
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "200.1",
				},
			}, []byte("this is body")),
		},
		{
			name: "parse int64 without srcType",
			fields: fields{
				key:     "a.b",
				srcType: "",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": int64(200),
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "200",
				},
			}, []byte("this is body")),
		},
		{
			name: "parse int64 with srcType",
			fields: fields{
				key:     "a.b",
				srcType: "int64",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": int64(200),
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "200",
				},
			}, []byte("this is body")),
		},
		{
			name: "parse float64 without srcType",
			fields: fields{
				key:     "a.b",
				srcType: "",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": float64(200.1),
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "200.1",
				},
			}, []byte("this is body")),
		},
		{
			name: "parse float64 with srcType",
			fields: fields{
				key:     "a.b",
				srcType: "float64",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": float64(200.1),
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "200.1",
				},
			}, []byte("this is body")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &ConvertStr{
				key:     tt.fields.key,
				srcType: tt.fields.srcType,
			}
			err := s.act(tt.args.e)
			assertions.NoError(err)
			assertions.Equal(tt.want, tt.args.e)
		})
	}
}
