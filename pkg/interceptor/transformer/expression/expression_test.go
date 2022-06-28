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

package expression

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseExpression(t *testing.T) {
	assertions := assert.New(t)

	type args struct {
		expression string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		want    *Expression
	}{
		{
			name: "no args",
			args: args{
				expression: "testName()",
			},
			want: &Expression{
				Name: "testName",
				Args: nil,
			},
		},
		{
			name: "one args",
			args: args{
				expression: "testName(foo)",
			},
			want: &Expression{
				Name: "testName",
				Args: []string{"foo"},
			},
		},
		{
			name: "args with comma",
			args: args{
				expression: "testName(foo, bar)",
			},
			wantErr: false,
			want: &Expression{
				Name: "testName",
				Args: []string{"foo", "bar"},
			},
		},
		{
			name: "missing last args",
			args: args{
				expression: "testName(foo, )",
			},
			wantErr: false,
			want: &Expression{
				Name: "testName",
				Args: []string{"foo"},
			},
		},
		{
			name: "args with {",
			args: args{
				expression: "testName(foo, {)",
			},
			wantErr: false,
			want: &Expression{
				Name: "testName",
				Args: []string{"foo", "{"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseExpression(tt.args.expression)
			assertions.Equal(tt.wantErr, err != nil)
			assertions.Equal(tt.want, got)
		})
	}
}
