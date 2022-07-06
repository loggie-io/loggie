package action

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTimestamp_act(t *testing.T) {
	assertions := assert.New(t)

	type fields struct {
		key   string
		extra *timestampExtra
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
			name: "time to unix ms",
			fields: fields{
				key: "a.b",
				extra: &timestampExtra{
					FromLayout:   "2006-01-02 15:04:05",
					FromLocation: "Asia/Shanghai",
					ToLayout:     "unix_ms",
					ToLocation:   "Local",
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "2022-06-28 11:24:35",
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": int64(1656386675000),
				},
			}, []byte("this is body")),
		},
		{
			name: "unix ms to utc timestamp",
			fields: fields{
				key: "a.b",
				extra: &timestampExtra{
					FromLocation: "Asia/Shanghai",
					FromLayout:   "unix_ms",
					ToLayout:     "2006-01-02 15:04:05",
					ToLocation:   "",
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "1656386675000",
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "2022-06-28 03:24:35",
				},
			}, []byte("this is body")),
		},
		{
			name: "unix ms to location Asia/Shanghai timestamp",
			fields: fields{
				key: "a.b",
				extra: &timestampExtra{
					FromLocation: "Asia/Shanghai",
					FromLayout:   "unix_ms",
					ToLayout:     "2006-01-02 15:04:05",
					ToLocation:   "Asia/Shanghai",
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "1656386675000",
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "2022-06-28 11:24:35",
				},
			}, []byte("this is body")),
		},
		{
			name: "timestamp layout convert",
			fields: fields{
				key: "a.b",
				extra: &timestampExtra{
					FromLayout:   "2006-01-02 15:04:05",
					FromLocation: "Asia/Shanghai",
					ToLayout:     time.RFC3339,
					ToLocation:   "Asia/Shanghai",
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "2022-06-28 11:24:35",
					},
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "2022-06-28T11:24:35+08:00",
				},
			}, []byte("this is body")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			t := &Timestamp{
				key:   tt.fields.key,
				extra: tt.fields.extra,
			}
			err := t.act(tt.args.e)
			assertions.NoError(err)
			assertions.Equal(tt.want, tt.args.e)
		})
	}
}
