package json

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/codec"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_pruneCLRF(t *testing.T) {
	type args struct {
		in []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "remove LF",
			args: args{
				in: []byte("hello loggie\n"),
			},
			want: []byte("hello loggie"),
		},
		{
			name: "remove CRLF",
			args: args{
				in: []byte("hello loggie\r\n"),
			},
			want: []byte("hello loggie"),
		},
		{
			name: "no CRLF",
			args: args{
				in: []byte("hello loggie"),
			},
			want: []byte("hello loggie"),
		},
		{
			name: "length is 1",
			args: args{
				in: []byte("h"),
			},
			want: []byte("h"),
		},
		{
			name: "length is 2",
			args: args{
				in: []byte("he"),
			},
			want: []byte("he"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pruneCLRF(tt.args.in)
			assert.Equal(t, got, tt.want)
		})
	}
}

func Test_decoder(t *testing.T) {
	log.InitDefaultLogger()
	var err error
	codecConfig := &codec.Config{}
	codecConfig.Type = Type
	PruneBValue := false
	regexcodec := Config{
		BodyFields: "log",
		Prune:      &PruneBValue,
	}
	j := &Json{
		config: &regexcodec,
	}
	j.Init()

	var e api.Event = event.NewEvent(map[string]interface{}{}, []byte("{\"log\":\"level=warn ts=2022-08-01T07:44:32.895Z caller=main.go:322 msg=\\\"unable to leave gossip mesh\\\" err=\\\"timeout waiting for leave broadcast\\\"\\n\",\"stream\":\"stderr\",\"time\":\"2022-08-01T07:44:32.896400037Z\"}"))
	eventData, err := j.Decode(e)
	assert.Equal(t, err, nil)
	assert.NotNil(t, eventData, nil)
	fmt.Println(eventData)
}
