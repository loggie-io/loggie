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

package codec

import (
	"reflect"
	"testing"
)

func TestInitMatcher(t *testing.T) {
	type args struct {
		pattern string
	}
	tests := []struct {
		name string
		args args
		want [][]string
	}{
		{
			name: "const",
			args: args{
				"aa-bb-cc",
			},
			want: nil,
		},
		{
			name: "ok",
			args: args{
				"aa-${bb}-${cc.dd}",
			},
			want: [][]string{
				{"${bb}", "bb"},
				{"${cc.dd}", "cc.dd"},
			},
		},
		{
			name: "ok-with-time",
			args: args{
				"aa-${bb}-${cc.dd}-${+YYYY.MM.DD}",
			},
			want: [][]string{
				{"${bb}", "bb"},
				{"${cc.dd}", "cc.dd"},
				{"${+YYYY.MM.DD}", "+YYYY.MM.DD"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InitMatcher(tt.args.pattern)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("InitMatcher() = %v, want %v", got, tt.want)
			}
		})
	}
}
