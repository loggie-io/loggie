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
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJsonDecode_Act(t *testing.T) {
	assertions := assert.New(t)

	type fields struct {
		key string
		to  string
	}
	type args struct {
		e api.Event
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    api.Event
	}{
		{
			name: "decode body to root",
			fields: fields{
				key: "body",
				to:  HeaderRoot,
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{}, []byte(`{"a":"b","c":"d"}`)),
			},
			wantErr: false,
			want: event.NewEvent(map[string]interface{}{
				"a": "b",
				"c": "d",
			}, []byte{}),
		},
		{
			name: "json decode field to itself",
			fields: fields{
				key: "k.v",
				to:  "k.v",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"k": map[string]interface{}{
						"v": `{"a":"b","c":"d"}`,
					},
				}, []byte("this is body")),
			},
			wantErr: false,
			want: event.NewEvent(map[string]interface{}{
				"k": map[string]interface{}{
					"v": map[string]interface{}{
						"a": "b",
						"c": "d",
					},
				},
			}, []byte("this is body")),
		},
		{
			name: "json decode body to header fields",
			fields: fields{
				key: "body",
				to:  "k",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{}, []byte(`{"a":"b","c":"d"}`)),
			},
			wantErr: false,
			want: event.NewEvent(map[string]interface{}{
				"k": map[string]interface{}{
					"a": "b",
					"c": "d",
				},
			}, []byte{}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &JsonDecode{
				key: tt.fields.key,
				to:  tt.fields.to,
			}
			err := j.act(tt.args.e)
			assertions.NoError(err)
			assertions.Equal(tt.want, tt.args.e)
		})
	}
}
