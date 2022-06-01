package normalize

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"reflect"
	"testing"
)

func TestGrokProcessor_Process(t *testing.T) {
	type fields struct {
		config *GrokConfig
	}
	type args struct {
		e api.Event
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[string]interface{}
	}{
		{
			name: "normal IgnoreBlank Overwrite UseDefaultPattern",
			fields: fields{config: &GrokConfig{
				Match: []string{
					"^%{DATESTAMP:datetime} (?P<file>[a-zA-Z0-9._-]+):%{INT:line}: %{IPV4:ip} %{PATH:path} %{UUID:uuid}(?P<space>[a-zA-Z]?)",
				},
				Target:            "body",
				IgnoreBlank:       true,
				Overwrite:         true,
				UseDefaultPattern: true,
			}},
			args: args{e: &event.DefaultEvent{
				H: map[string]interface{}{
					"file": "test.go",
				},
				B: []byte("2022/05/28 01:32:01 logTest.go:66: 192.168.0.1 /var/log/test.log 54ce5d87-b94c-c40a-74a7-9cd375289334"),
			}},
			want: map[string]interface{}{
				"datetime": "2022/05/28 01:32:01",
				"file":     "logTest.go",
				"line":     "66",
				"ip":       "192.168.0.1",
				"path":     "/var/log/test.log",
				"uuid":     "54ce5d87-b94c-c40a-74a7-9cd375289334",
			},
		},
		{
			name: "IgnoreBlank false",
			fields: fields{config: &GrokConfig{
				Match: []string{
					"^%{DATESTAMP:datetime} (?P<file>[a-zA-Z0-9._-]+):%{INT:line}: %{IPV4:ip} %{PATH:path} %{UUID:uuid}(?P<space>[a-zA-Z]?)",
				},
				Target:            "body",
				IgnoreBlank:       false,
				Overwrite:         true,
				UseDefaultPattern: true,
			}},
			args: args{e: &event.DefaultEvent{
				H: map[string]interface{}{
					"file": "test.go",
				},
				B: []byte("2022/05/28 01:32:01 logTest.go:66: 192.168.0.1 /var/log/test.log 54ce5d87-b94c-c40a-74a7-9cd375289334"),
			}},
			want: map[string]interface{}{
				"datetime": "2022/05/28 01:32:01",
				"file":     "logTest.go",
				"line":     "66",
				"ip":       "192.168.0.1",
				"path":     "/var/log/test.log",
				"uuid":     "54ce5d87-b94c-c40a-74a7-9cd375289334",
				"space":    "",
			},
		},
		{
			name: "Overwrite false",
			fields: fields{config: &GrokConfig{
				Match: []string{
					"^%{DATESTAMP:datetime} (?P<file>[a-zA-Z0-9._-]+):%{INT:line}: %{IPV4:ip} %{PATH:path} %{UUID:uuid}(?P<space>[a-zA-Z]?)",
				},
				Target:            "body",
				IgnoreBlank:       true,
				Overwrite:         false,
				UseDefaultPattern: true,
			}},
			args: args{e: &event.DefaultEvent{
				H: map[string]interface{}{
					"file": "test.go",
				},
				B: []byte("2022/05/28 01:32:01 logTest.go:66: 192.168.0.1 /var/log/test.log 54ce5d87-b94c-c40a-74a7-9cd375289334"),
			}},
			want: map[string]interface{}{
				"datetime": "2022/05/28 01:32:01",
				"file":     "test.go",
				"line":     "66",
				"ip":       "192.168.0.1",
				"path":     "/var/log/test.log",
				"uuid":     "54ce5d87-b94c-c40a-74a7-9cd375289334",
			},
		},
		{
			name: "target is head",
			fields: fields{config: &GrokConfig{
				Match: []string{
					"^%{DATESTAMP:datetime} (?P<file>[a-zA-Z0-9._-]+):%{INT:line}: %{IPV4:ip} %{PATH:path} %{UUID:uuid}(?P<space>[a-zA-Z]?)",
				},
				Target:            "info",
				IgnoreBlank:       true,
				Overwrite:         true,
				UseDefaultPattern: true,
			}},
			args: args{e: &event.DefaultEvent{
				H: map[string]interface{}{
					"file": "test.go",
					"info": "2022/05/28 01:32:01 logTest.go:66: 192.168.0.1 /var/log/test.log 54ce5d87-b94c-c40a-74a7-9cd375289334",
				},
				B: []byte("just body"),
			}},
			want: map[string]interface{}{
				"datetime": "2022/05/28 01:32:01",
				"file":     "logTest.go",
				"line":     "66",
				"ip":       "192.168.0.1",
				"path":     "/var/log/test.log",
				"uuid":     "54ce5d87-b94c-c40a-74a7-9cd375289334",
				"info":     "2022/05/28 01:32:01 logTest.go:66: 192.168.0.1 /var/log/test.log 54ce5d87-b94c-c40a-74a7-9cd375289334",
			},
		},
		{
			name: "use dst",
			fields: fields{config: &GrokConfig{
				Match: []string{
					"^%{DATESTAMP:datetime} (?P<file>[a-zA-Z0-9._-]+):%{INT:line}: %{IPV4:ip} %{PATH:path} %{UUID:uuid}(?P<space>[a-zA-Z]?)",
				},
				Target:            "body",
				IgnoreBlank:       true,
				Overwrite:         true,
				UseDefaultPattern: true,
				Dst:               "grok_output",
			}},
			args: args{e: &event.DefaultEvent{
				H: map[string]interface{}{
					"file": "test.go",
				},
				B: []byte("2022/05/28 01:32:01 logTest.go:66: 192.168.0.1 /var/log/test.log 54ce5d87-b94c-c40a-74a7-9cd375289334"),
			}},
			want: map[string]interface{}{
				"file": "test.go",
				"grok_output": map[string]interface{}{
					"datetime": "2022/05/28 01:32:01",
					"file":     "logTest.go",
					"line":     "66",
					"ip":       "192.168.0.1",
					"path":     "/var/log/test.log",
					"uuid":     "54ce5d87-b94c-c40a-74a7-9cd375289334",
				},
			},
		},
		{
			name: "use Pattern by user",
			fields: fields{config: &GrokConfig{
				Match: []string{
					"^%{DATESTAMP:datetime} %{FILE:file}:%{INT:line}: %{IPV4:ip} %{PATH:path} %{UUID:uuid}(?P<space>[a-zA-Z]?)",
				},
				Target:            "body",
				IgnoreBlank:       true,
				Overwrite:         true,
				UseDefaultPattern: true,
				Pattern: map[string]string{
					"FILE": "[a-zA-Z0-9._-]+",
				},
			}},
			args: args{e: &event.DefaultEvent{
				H: map[string]interface{}{
					"file": "test.go",
				},
				B: []byte("2022/05/28 01:32:01 logTest.go:66: 192.168.0.1 /var/log/test.log 54ce5d87-b94c-c40a-74a7-9cd375289334"),
			}},
			want: map[string]interface{}{
				"datetime": "2022/05/28 01:32:01",
				"file":     "logTest.go",
				"line":     "66",
				"ip":       "192.168.0.1",
				"path":     "/var/log/test.log",
				"uuid":     "54ce5d87-b94c-c40a-74a7-9cd375289334",
			},
		},
		{
			name: "use user Pattern to overwrite DefaultPattern",
			fields: fields{config: &GrokConfig{
				Match: []string{
					"^%{DATESTAMP:datetime} %{WORD:file}:%{INT:line}: %{IPV4:ip} %{PATH:path} %{UUID:uuid}(?P<space>[a-zA-Z]?)",
				},
				Target:            "body",
				IgnoreBlank:       true,
				Overwrite:         true,
				UseDefaultPattern: true,
				Pattern: map[string]string{
					"WORD": "[a-zA-Z0-9._-]+",
				},
			}},
			args: args{e: &event.DefaultEvent{
				H: map[string]interface{}{
					"file": "test.go",
				},
				B: []byte("2022/05/28 01:32:01 logTest.go:66: 192.168.0.1 /var/log/test.log 54ce5d87-b94c-c40a-74a7-9cd375289334"),
			}},
			want: map[string]interface{}{
				"datetime": "2022/05/28 01:32:01",
				"file":     "logTest.go",
				"line":     "66",
				"ip":       "192.168.0.1",
				"path":     "/var/log/test.log",
				"uuid":     "54ce5d87-b94c-c40a-74a7-9cd375289334",
			},
		},
		// sometime this test case may get error cause by the url connection refused
		//{
		//	name: "use Pattern from url",
		//	fields: fields{config: &GrokConfig{
		//		Match: []string{
		//			"^%{DATESTAMP:datetime} (?P<file>[a-zA-Z0-9._-]+):%{INT:line}: %{IPV4:ip} %{PATH:path} %{UUID:uuid}(?P<space>[a-zA-Z]?)",
		//		},
		//		Target:            "body",
		//		IgnoreBlank:       true,
		//		Overwrite:         true,
		//		UseDefaultPattern: false,
		//		PatternPaths:      []string{"https://raw.githubusercontent.com/vjeantet/grok/master/patterns/grok-patterns"},
		//	}},
		//	args: args{e: &event.DefaultEvent{
		//		H: map[string]interface{}{
		//			"file": "test.go",
		//		},
		//		B: []byte("05/28/2022 01:32:01 logTest.go:66: 192.168.0.1 /var/log/test.log 54ce5d87-b94c-c40a-74a7-9cd375289334"),
		//	}},
		//	want: map[string]interface{}{
		//		"datetime": "05/28/2022 01:32:01",  // this url just support DATE_US and DATE_EU
		//		"file":     "logTest.go",
		//		"line":     "66",
		//		"ip":       "192.168.0.1",
		//		"path":     "/var/log/test.log",
		//		"uuid":     "54ce5d87-b94c-c40a-74a7-9cd375289334",
		//	},
		//},
		{
			/*  Patterns.txt:
			USERNAME  [a-zA-Z0-9._-]+
			USER      %{USERNAME}
			INT       (?:[+-]?(?:[0-9]+))
			WORD      \b\w+\b
			UUID      [A-Fa-f0-9]{8}-(?:[A-Fa-f0-9]{4}-){3}[A-Fa-f0-9]{12}
			IPV4      (?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)
			PATH      (?:%{UNIXPATH}|%{WINPATH})
			UNIXPATH  (/[\w_%!$@:.,-]?/?)(\S+)?
			WINPATH   ([A-Za-z]:|\\)(?:\\[^\\?*]*)+
			MONTHNUM  (?:0?[1-9]|1[0-2])
			MONTHDAY  (?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])
			YEAR      (\d\d){1,2}
			DATE_US   %{MONTHNUM}[/-]%{MONTHDAY}[/-]%{YEAR}
			DATE_EU   %{MONTHDAY}[./-]%{MONTHNUM}[./-]%{YEAR}
			DATE_CN   %{YEAR}[./-]%{MONTHNUM}[./-]%{MONTHDAY}
			DATE      %{DATE_US}|%{DATE_EU}|%{DATE_CN}
			HOUR      (?:2[0123]|[01]?[0-9])
			MINUTE    (?:[0-5][0-9])
			SECOND    (?:(?:[0-5][0-9]|60)(?:[:.,][0-9]+)?)
			TIME      ([^0-9]?)%{HOUR}:%{MINUTE}(?::%{SECOND})([^0-9]?)
			DATESTAMP %{DATE}[- ]%{TIME}
			*/
			name: "use Pattern from local path",
			fields: fields{config: &GrokConfig{
				Match: []string{
					"^%{DATESTAMP:datetime} (?P<file>[a-zA-Z0-9._-]+):%{INT:line}: %{IPV4:ip} %{PATH:path} %{UUID:uuid}(?P<space>[a-zA-Z]?)",
				},
				Target:            "body",
				IgnoreBlank:       true,
				Overwrite:         true,
				UseDefaultPattern: false,
				PatternPaths:      []string{"./Patterns.txt"},
			}},
			args: args{e: &event.DefaultEvent{
				H: map[string]interface{}{
					"file": "test.go",
				},
				B: []byte("2022/05/28 01:32:01 logTest.go:66: 192.168.0.1 /var/log/test.log 54ce5d87-b94c-c40a-74a7-9cd375289334"),
			}},
			want: map[string]interface{}{
				"datetime": "2022/05/28 01:32:01", // this url just support DATE_US and DATE_EU
				"file":     "logTest.go",
				"line":     "66",
				"ip":       "192.168.0.1",
				"path":     "/var/log/test.log",
				"uuid":     "54ce5d87-b94c-c40a-74a7-9cd375289334",
			},
		},
	}

	log.InitDefaultLogger()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &GrokProcessor{
				config: tt.fields.config,
			}
			p.Init()
			_ = p.Process(tt.args.e)
			if !reflect.DeepEqual(tt.want, tt.args.e.Header()) {
				t.Errorf("Process() got = %v, want=%v", tt.args.e.Header(), tt.want)
			}
		})
	}
}
