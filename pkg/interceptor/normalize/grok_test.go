package normalize

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"reflect"
	"regexp"
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
			name: "normal",
			fields: fields{config: &GrokConfig{
				Match: []string{
					"^%{DATESTAMP:datetime} %{IPV4:ip} %{PATH:path} %{WORD:word} %{INT:int} %{UUID:uuid}",
				},
				Target:            "body",
				UseDefaultPattern: true,
			}},
			args: args{e: &event.DefaultEvent{
				H: map[string]interface{}{},
				B: []byte("2022-05-26 21:00:00 127.0.0.1 /var/log/test.log someworld 66 550e8400-e29b-41d4-a716-446655440000"),
			}},
			want: map[string]interface{}{
				"datetime": "2022-05-26 21:00:00",
				"ip":       "127.0.0.1",
				"path":     "/var/log/test.log",
				"word":     "someworld",
				"int":      "66",
				"uuid":     "550e8400-e29b-41d4-a716-446655440000",
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
			finalPattern := p.groks[0].translateMatchPattern(tt.fields.config.Match[0])
			pnew, err := regexp.Compile(finalPattern)
			if err != nil {
				log.Error("could not build Grok:%s", err)
			}
			p.groks[0].p = pnew
			p.groks[0].subexpNames = pnew.SubexpNames()

			_ = p.Process(tt.args.e)
			if !reflect.DeepEqual(tt.want, tt.args.e.Header()) {
				t.Errorf("Process() got = %v, want=%v", tt.args.e.Header(), tt.want)
			}
		})
	}
}
