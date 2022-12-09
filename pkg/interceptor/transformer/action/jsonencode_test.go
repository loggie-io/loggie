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

func TestJsonEncode_Act(t *testing.T) {
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
			name: "encode body",
			fields: fields{
				key: "body",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{}, []byte{}),
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "json encode field to itself",
			fields: fields{
				key: "k",
				to:  "k",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"k": map[string]interface{}{
						"str1": "str1024",
					},
				}, []byte{}),
			},
			wantErr: false,
			want: event.NewEvent(map[string]interface{}{
				"k": `{"str1":"str1024"}`,
			}, []byte{}),
		},
		{
			name: "json encode field to root",
			fields: fields{
				key: "k",
				to:  HeaderRoot,
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"k": map[string]interface{}{
						"str1": "str1024",
					},
				}, []byte{}),
			},
			wantErr: false,
			want: event.NewEvent(map[string]interface{}{
				"k": `{"str1":"str1024"}`,
			}, []byte{}),
		},
		{
			name: "json encode field to fields",
			fields: fields{
				key: "k",
				to:  "k1",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"k": map[string]interface{}{
						"str1": "str1024",
					},
				}, []byte{}),
			},
			wantErr: false,
			want: event.NewEvent(map[string]interface{}{
				"k1": `{"str1":"str1024"}`,
			}, []byte{}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j, err := NewJsonEncode([]string{tt.fields.key, tt.fields.to})
			if tt.wantErr {
				assertions.NotNil(err)
			} else {
				assertions.NoError(err)
			}

			if err != nil {
				return
			}

			err = j.act(tt.args.e)
			if tt.wantErr {
				assertions.NotNil(err)
			} else {
				assertions.NoError(err)
			}
			assertions.Equal(tt.want, tt.args.e)
		})
	}
}
