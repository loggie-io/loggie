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

package cfg

import (
	"reflect"
	"testing"
)

func TestPack(t *testing.T) {
	type config struct {
		Code string `yaml:"code"`
		Name string `yaml:"name"`
	}
	type args struct {
		config interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    CommonCfg
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				config: &config{
					Name: "test",
					Code: "test",
				},
			},
			want: CommonCfg{
				"name": "test",
				"code": "test",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Pack(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("Pack() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Pack() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMergeCommonCfg(t *testing.T) {
	type args struct {
		base     CommonCfg
		from     CommonCfg
		override bool
	}
	tests := []struct {
		name string
		args args
		want CommonCfg
	}{
		{
			name: "merge_success with override true",
			args: args{
				base: CommonCfg{
					"type": "file",
					"name": "aa",
					"path": []string{
						"stdout",
					},
				},
				from: CommonCfg{
					"path": []string{
						"/var/log/*.log",
					},
					"extra": "foo",
				},
				override: true,
			},
			want: CommonCfg{
				"type": "file",
				"name": "aa",
				"path": []string{
					"/var/log/*.log",
				},
				"extra": "foo",
			},
		},
		{
			name: "merge_success with override false",
			args: args{
				base: CommonCfg{
					"type": "file",
					"name": "aa",
					"path": []string{
						"stdout",
					},
				},
				from: CommonCfg{
					"path": []string{
						"/var/log/*.log",
					},
					"extra": "foo",
				},
				override: false,
			},
			want: CommonCfg{
				"type": "file",
				"name": "aa",
				"path": []string{
					"stdout",
				},
				"extra": "foo",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MergeCommonCfg(tt.args.base, tt.args.from, tt.args.override); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MergeCommonCfg() = %v, want %v", got, tt.want)
			}
		})
	}
}
