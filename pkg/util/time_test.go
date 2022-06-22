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

package util

import (
	"github.com/goccy/go-yaml"
	"testing"
	"time"
)

func TestTimeFormatNow(t *testing.T) {
	type args struct {
		pattern string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "ok",
			args: args{
				"YYYY-MM-DD",
			},
			want: time.Now().Format("2006-01-02"),
		},
		{
			name: "ok-hour",
			args: args{
				"YYYY-MM-DD:hh",
			},
			want: time.Now().Format("2006-01-02:15"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TimeFormatNow(tt.args.pattern)
			if got != tt.want {
				t.Errorf("TimeFormatNow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseDuration(t *testing.T) {
	type config struct {
		Duration Duration `yaml:"duration,omitempty"`
	}

	configStr := `duration: "1d"`
	var c config
	err := yaml.Unmarshal([]byte(configStr), &c)
	if err != nil {
		t.Errorf("yaml unmarshal fail, error: %s", err.Error())
	}

	oneDay, _ := time.ParseDuration("24h")
	if c.Duration.Duration() != oneDay {
		t.Errorf("Expected to be equal: %s vs %s", oneDay, c.Duration.Duration())
	}
}
