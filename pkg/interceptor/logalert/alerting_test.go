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

package logalert

import (
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/event"
	"testing"
)

func TestInterceptor_match(t *testing.T) {
	type fields struct {
		config *Config
	}
	type args struct {
		event api.Event
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantMatched bool
	}{
		{
			name: "containers string",
			fields: fields{
				config: &Config{
					Matcher: Matcher{
						Contains: []string{"error", "Error"},
					},
				},
			},
			args: args{
				event: event.NewEvent(map[string]interface{}{}, []byte("2030-0704 Error this is a test message")),
			},
			wantMatched: true,
		},
		{
			name: "regexp matched",
			fields: fields{
				config: &Config{
					Matcher: Matcher{
						Regexp: []string{"\\berror\\b", "\\bError\\b"},
					},
				},
			},
			args: args{
				event: event.NewEvent(map[string]interface{}{}, []byte("2030-0704 Error this is a test message")),
			},
			wantMatched: true,
		},
		{
			name: "from header matched",
			fields: fields{
				config: &Config{
					Matcher: Matcher{
						Regexp:       []string{"\\berror\\b", "\\bError\\b"},
						TargetHeader: "msg",
					},
				},
			},
			args: args{
				event: event.NewEvent(map[string]interface{}{
					"msg": "2030-0704 Error this is a test message",
				}, []byte("")),
			},
			wantMatched: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Interceptor{
				config: tt.fields.config,
			}
			gotMatched, reason, message := i.match(tt.args.event)
			t.Logf("match() gotMatched = %v, want %v, reason: %s, message: %s", gotMatched, tt.wantMatched, reason, message)
			if gotMatched != tt.wantMatched {
				t.Errorf("match() gotMatched = %v, want %v", gotMatched, tt.wantMatched)
			}
		})
	}
}
