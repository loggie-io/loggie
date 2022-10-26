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
	"github.com/loggie-io/loggie/pkg/interceptor/logalert/condition"
	"github.com/loggie-io/loggie/pkg/util"
	"regexp"
	"testing"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
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
		{
			name: "containers string",
			fields: fields{
				config: &Config{
					Advanced: Advanced{
						Enable:    true,
						Mode:      []string{"regexp"},
						Duration:  0,
						MatchType: "any",
						Rules: []Rule{
							{
								Regexp:   `(?<date>.*?) (?<time>[\S|\\.]+)  (?<status>[\S|\\.]+) (?<u>.*?) --- (?<thread>\[.*?\]) (?<pkg>.*) : (?<message>.*)`,
								Key:      "status",
								Operator: "eq",
								Value:    "WARN",
							},
						},
					},
				},
			},
			args: args{
				event: event.NewEvent(map[string]interface{}{}, []byte("2022-10-26 09:39:24.101  WARN 98019 --- [           main] o.apache.catalina.core.StandardService   : Starting service [Tomcat]")),
			},
			wantMatched: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Interceptor{
				config: tt.fields.config,
			}

			var regex []*regexp.Regexp
			for _, reg := range tt.fields.config.Matcher.Regexp {
				regex = append(regex, regexp.MustCompile(reg))
			}
			i.regex = regex

			if i.config.Advanced.Enable {
				for _, mode := range i.config.Advanced.Mode {
					if mode == ModeRegexp {
						for _, rule := range i.config.Advanced.Rules {
							regex := util.MustCompilePatternWithJavaStyle(rule.Regexp)
							aRule := advancedRule{
								regex:        regex,
								key:          rule.Key,
								operatorFunc: condition.OperatorMap[rule.Operator],
								target:       rule.Value,
							}
							i.rules = append(i.rules, aRule)
						}
					}
				}

			}

			gotMatched, reason, message := i.match(tt.args.event)
			t.Logf("match() gotMatched = %v, want %v, reason: %s, message: %s", gotMatched, tt.wantMatched, reason, message)
			if gotMatched != tt.wantMatched {
				t.Errorf("match() gotMatched = %v, want %v", gotMatched, tt.wantMatched)
			}
		})
	}
}
