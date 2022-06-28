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

package condition

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHasPrefix_Check(t *testing.T) {
	assertions := assert.New(t)

	type fields struct {
		field  string
		prefix string
	}
	type args struct {
		e api.Event
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "ok-header",
			fields: fields{
				field:  "a.b",
				prefix: "{",
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "{expect json}",
					},
				}, []byte("this is body")),
			},
			want: true,
		},
		{
			name: "ok-body",
			fields: fields{
				field:  "body",
				prefix: "{",
			},
			args: args{
				e: event.NewEvent(nil, []byte("{this is body}")),
			},
			want: true,
		},
		{
			name: "not match body",
			fields: fields{
				field:  "body",
				prefix: "{",
			},
			args: args{
				e: event.NewEvent(nil, []byte("this is body")),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &HasPrefix{
				field:  tt.fields.field,
				prefix: tt.fields.prefix,
			}
			got := p.Check(tt.args.e)
			assertions.Equal(tt.want, got, "check failed")
		})
	}
}
