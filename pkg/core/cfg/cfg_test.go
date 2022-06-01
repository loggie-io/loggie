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

func TestCommonCfg_GetProperties(t *testing.T) {
	tests := []struct {
		name string
		c    CommonCfg
		want CommonCfg
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.GetProperties(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CommonCfg.GetProperties() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUnpackFromFileDefaultsAndValidate(t *testing.T) {
	type args struct {
		path   string
		config interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := UnpackFromFileDefaultsAndValidate(tt.args.path, tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("UnpackFromFileDefaultsAndValidate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUnpackFromFile(t *testing.T) {
	type args struct {
		path   string
		config interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := UnpackFromFile(tt.args.path, tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("UnpackFromFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUnpackRawDefaultsAndValidate(t *testing.T) {
	type args struct {
		content []byte
		config  interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := UnpackRawDefaultsAndValidate(tt.args.content, tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("UnpackRawDefaultsAndValidate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUnpackRaw(t *testing.T) {
	type args struct {
		content []byte
		config  interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := UnpackRaw(tt.args.content, tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("UnpackRaw() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUnpackRawAndDefaults(t *testing.T) {
	type args struct {
		content []byte
		config  interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := UnpackRawAndDefaults(tt.args.content, tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("UnpackRawAndDefaults() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUnpackDefaultsAndValidate(t *testing.T) {
	type args struct {
		properties CommonCfg
		config     interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := UnpackDefaultsAndValidate(tt.args.properties, tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("UnpackDefaultsAndValidate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUnpackAndDefaults(t *testing.T) {
	type args struct {
		properties CommonCfg
		config     interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := UnpackAndDefaults(tt.args.properties, tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("UnpackAndDefaults() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

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

func Test_setDefault(t *testing.T) {
	type args struct {
		config interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := setDefault(tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("setDefault() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_validate(t *testing.T) {
	type args struct {
		config interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validate(tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMergeCommonCfg(t *testing.T) {
	type args struct {
		base CommonCfg
		from CommonCfg
	}
	tests := []struct {
		name string
		args args
		want CommonCfg
	}{
		{
			name: "merge_success",
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
				},
			},
			want: CommonCfg{
				"type": "file",
				"name": "aa",
				"path": []string{
					"/var/log/*.log",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MergeCommonCfg(tt.args.base, tt.args.from, true); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MergeCommonCfg() = %v, want %v", got, tt.want)
			}
		})
	}
}
