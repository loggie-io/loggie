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
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFmt_act(t *testing.T) {
	assertions := assert.New(t)

	type fields struct {
		key   string
		p     *pattern.Pattern
		extra *fmtExtra
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
			name: "fmt header",
			fields: fields{
				key: "a.b",
				p:   pattern.MustInit("new ${a.b}"),
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
					},
				}, []byte("body message")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "new foo",
				},
			}, []byte("body message")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Fmt{
				key:   tt.fields.key,
				p:     tt.fields.p,
				extra: tt.fields.extra,
			}
			err := f.act(tt.args.e)
			assertions.NoError(err)
			assertions.Equal(tt.want, tt.args.e)
		})
	}
}
