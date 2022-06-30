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
	"github.com/loggie-io/loggie/pkg/util/eventops"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLess_Check(t *testing.T) {
	assertions := assert.New(t)

	var n1 int
	n1 = 520
	n1Num, err := eventops.NewNumber(n1)
	assertions.NoError(err, "new number")

	type fields struct {
		field string
		value *eventops.Number
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
			name: "field value is int",
			fields: fields{
				field: "a.b",
				value: n1Num,
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": int(500),
					},
				}, []byte("this is body")),
			},
			want: true,
		},
		{
			name: "field value is string",
			fields: fields{
				field: "a.b",
				value: n1Num,
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "200",
					},
				}, []byte("this is body")),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eq := &Less{
				field: tt.fields.field,
				value: tt.fields.value,
			}
			got := eq.Check(tt.args.e)
			assertions.Equal(tt.want, got, "check failed")
		})
	}
}
