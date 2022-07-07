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

package json

import (
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
)

func TestJson_Encode(t *testing.T) {
	type args struct {
		event api.Event
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "jsonOK",
			args: args{
				event: event.NewEvent(map[string]interface{}{
					"a": "b",
					"c": map[string]interface{}{
						"d": "e",
					},
				}, []byte("this is body")),
			},
			want: []byte(`{"a":"b","body":"this is body","c":{"d":"e"}}`),
		},
		{
			name: "BodyInHeaderOK",
			args: args{
				event: event.NewEvent(map[string]interface{}{
					"a": "b",
					"c": map[string]interface{}{
						"d": "e",
					},
					"body": "this is message",
				}, []byte{}),
			},
			want: []byte(`{"a":"b","c":{"d":"e"},"body":"this is message"}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Json{
				config: &Config{
					Pretty:      false,
					BeatsFormat: false,
				},
			}
			got, err := j.Encode(tt.args.event)
			assert.NoError(t, err)
			assert.JSONEq(t, string(tt.want), string(got))
		})
	}
}
