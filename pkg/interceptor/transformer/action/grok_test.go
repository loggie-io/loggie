package action

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_grokAction_act(t *testing.T) {
	type fields struct {
		config *grokConfig
		key    string
	}
	type args struct {
		e api.Event
	}
	T := true
	F := false
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[string]interface{}
	}{
		{
			name: "normal IgnoreBlank Overwrite UseDefaultPattern",
			fields: fields{config: &grokConfig{
				Match:       "^%{DATESTAMP:datetime} (?P<file>[a-zA-Z0-9._-]+):%{INT:line}: %{IPV4:ip} %{PATH:path} %{UUID:uuid}(?P<space>[a-zA-Z]?)",
				IgnoreBlank: &T,
			},
				key: "body",
			},
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
			fields: fields{config: &grokConfig{
				Match:       "^%{DATESTAMP:datetime} (?P<file>[a-zA-Z0-9._-]+):%{INT:line}: %{IPV4:ip} %{PATH:path} %{UUID:uuid}(?P<space>[a-zA-Z]?)",
				IgnoreBlank: &F,
			},
				key: "body",
			},
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
			name: "use Pattern by user",
			fields: fields{config: &grokConfig{
				Match:       "^%{DATESTAMP:datetime} %{FILE:file}:%{INT:line}: %{IPV4:ip} %{PATH:path} %{UUID:uuid}(?P<space>[a-zA-Z]?)",
				IgnoreBlank: &T,
				Pattern: map[string]string{
					"FILE": "[a-zA-Z0-9._-]+",
				},
			},
				key: "body",
			},
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
			fields: fields{config: &grokConfig{
				Match:       "^%{DATESTAMP:datetime} %{WORD:file}:%{INT:line}: %{IPV4:ip} %{PATH:path} %{UUID:uuid}(?P<space>[a-zA-Z]?)",
				IgnoreBlank: &T,
				Pattern: map[string]string{
					"WORD": "[a-zA-Z0-9._-]+",
				},
			},
				key: "body",
			},
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
	}

	log.InitDefaultLogger()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &grokAction{
				key:    tt.fields.key,
				to:     HeaderRoot,
				config: tt.fields.config,
			}
			p.initGrok()
			err := p.act(tt.args.e)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, tt.args.e.Header())
		})
	}
}
