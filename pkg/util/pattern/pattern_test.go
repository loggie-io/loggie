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

package pattern

import (
	"reflect"
	"testing"
)

func TestExtract(t *testing.T) {
	type args struct {
		input     string
		splitsStr []string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "ok-1",
			args: args{
				input: "/var/d3/dade_svc-adx/sa.log",
				splitsStr: []string{
					"/var/d3/", "_", "/",
				},
			},
			want: []string{
				"dade", "svc-adx",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Extract(tt.args.input, tt.args.splitsStr); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Extract() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetSplits(t *testing.T) {
	type args struct {
		target string
	}
	tests := []struct {
		name         string
		args         args
		wantSplitStr []string
		wantMatchers []string
	}{
		{
			name: "ok-1",
			args: args{
				target: "${name}/${uid}_${ns}",
			},
			wantSplitStr: []string{
				"/", "_",
			},
			wantMatchers: []string{
				"name", "uid", "ns",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSplitStr, gotMatchers := GetSplits(tt.args.target)
			if !reflect.DeepEqual(gotSplitStr, tt.wantSplitStr) {
				t.Errorf("GetSplits() gotSplitStr = %v, want %v", gotSplitStr, tt.wantSplitStr)
			}
			if !reflect.DeepEqual(gotMatchers, tt.wantMatchers) {
				t.Errorf("GetSplits() gotMatchers = %v, want %v", gotMatchers, tt.wantMatchers)
			}
		})
	}
}
