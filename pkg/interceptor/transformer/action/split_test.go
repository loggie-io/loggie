package action

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSplit_act(t *testing.T) {
	log.InitDefaultLogger()
	type fields struct {
		key   string
		to    string
		extra *SplitExtra
	}
	type args struct {
		e api.Event
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   api.Event
	}{
		{
			name: "split body",
			fields: fields{
				key: "body",
				to:  HeaderRoot,
				extra: &SplitExtra{
					Separator: "|",
					Max:       -1,
					Keys:      []string{"time", "order", "service", "price"},
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{}, []byte(`2021-08-08|U12345|storeCenter|13.14`)),
			},
			want: event.NewEvent(map[string]interface{}{
				"time":    "2021-08-08",
				"order":   "U12345",
				"service": "storeCenter",
				"price":   "13.14",
			}, []byte{}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Split{
				key:   tt.fields.key,
				to:    tt.fields.to,
				extra: tt.fields.extra,
			}
			err := r.act(tt.args.e)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, tt.args.e)

		})
	}
}
