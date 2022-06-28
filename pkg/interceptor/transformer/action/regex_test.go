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
	"github.com/loggie-io/loggie/pkg/util"
	"regexp"
	"testing"
)

func TestRegex_act(t *testing.T) {

	const pattern = `(?<ip>\S+) (?<id>\S+) (?<u>\S+) (?<time>\[.*?\]) (?<url>\".*?\") (?<status>\S+) (?<size>\S+)`

	type fields struct {
		key   string
		to    string
		reg   *regexp.Regexp
		extra *regexExtra
	}
	type args struct {
		e api.Event
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    api.Event
	}{
		{
			name: "regex body",
			fields: fields{
				key: "body",
				to:  HeaderRoot,
				reg: util.MustCompilePatternWithJavaStyle(pattern),
				extra: &regexExtra{
					Pattern: pattern,
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{}, []byte(`10.244.0.1 - - [13/Dec/2021:12:40:48 +0000] "GET / HTTP/1.1" 404 683`)),
			},
			wantErr: false,
			want: event.NewEvent(map[string]interface{}{
				"ip":     "10.244.0.1",
				"id":     "-",
				"u":      "-",
				"time":   "[13/Dec/2021:12:40:48 +0000]",
				"url":    "\"GET / HTTP/1.1\"",
				"status": "404",
				"size":   "683",
			}, []byte{}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Regex{
				key:   tt.fields.key,
				to:    tt.fields.to,
				reg:   tt.fields.reg,
				extra: tt.fields.extra,
			}
			if err := r.act(tt.args.e); (err != nil) != tt.wantErr {
				t.Errorf("act() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
