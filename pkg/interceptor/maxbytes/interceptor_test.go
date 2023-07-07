package maxbytes

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_maxBytes(t *testing.T) {
	type args struct {
		event    api.Event
		target   string
		maxBytes int
	}
	tests := []struct {
		name string
		args args
		want api.Event
	}{
		{
			name: "truncate body OK",
			args: args{
				event: event.NewEvent(map[string]interface{}{
					"foo": "bar",
				}, []byte("this is a test log.")),
				target:   event.Body,
				maxBytes: 5,
			},
			want: event.NewEvent(map[string]interface{}{
				"foo": "bar",
			}, []byte("this ")),
		},
		{
			name: "truncate fields OK",
			args: args{
				event: event.NewEvent(map[string]interface{}{
					"foo": "this is a test log.",
				}, []byte("")),
				target:   "foo",
				maxBytes: 5,
			},
			want: event.NewEvent(map[string]interface{}{
				"foo": "this ",
			}, []byte("")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxBytes(tt.args.event, tt.args.target, tt.args.maxBytes)
			assert.Equal(t, tt.args.event.Body(), tt.want.Body())
			assert.Equal(t, tt.args.event.Header(), tt.want.Header())
		})
	}
}
