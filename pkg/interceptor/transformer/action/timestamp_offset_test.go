package action

import (
	"testing"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/stretchr/testify/assert"
)

func TestTimestampOffset_act(t *testing.T) {
	assertions := assert.New(t)

	type fields struct {
		key   string
		extra *timestampOffsetExtra
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
			name: "unix_ms to string with 5m offset",
			fields: fields{
				key: "ts",
				extra: &timestampOffsetExtra{
					FromLayout:     "unix_ms",
					ToLayout:       "2006-01-02 15:04:05",
					ToLocation:     "Asia/Shanghai",
					BeforeKey:      "ts_before",
					AfterKey:       "ts_after",
					OffsetDuration: "5m",
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"ts": "1656386675000", // 2022-06-28 11:24:35 CST
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"ts":        "2022-06-28 11:24:35",
				"ts_before": "2022-06-28 11:19:35",
				"ts_after":  "2022-06-28 11:29:35",
			}, []byte("this is body")),
		},
		{
			name: "unix_ms to unix with 10m offset",
			fields: fields{
				key: "ts",
				extra: &timestampOffsetExtra{
					FromLayout:     "unix_ms",
					ToLayout:       "unix",
					ToLocation:     "UTC",
					BeforeKey:      "ts_before",
					AfterKey:       "ts_after",
					OffsetDuration: "10m",
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"ts": "1656386675000", // 2022-06-28 03:24:35 UTC, unix 1656386675
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"ts":        int64(1656386675),
				"ts_before": int64(1656386675 - 600),
				"ts_after":  int64(1656386675 + 600),
			}, []byte("this is body")),
		},
		{
			name: "string to string with 1h offset and timezone",
			fields: fields{
				key: "ts",
				extra: &timestampOffsetExtra{
					FromLayout:     "2006-01-02 15:04:05",
					FromLocation:   "Asia/Shanghai",
					ToLayout:       "2006-01-02 15:04:05",
					ToLocation:     "UTC",
					BeforeKey:      "ts_before",
					AfterKey:       "ts_after",
					OffsetDuration: "1h",
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"ts": "2022-06-28 12:00:00",
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"ts":        "2022-06-28 04:00:00", // 12:00 CST = 04:00 UTC
				"ts_before": "2022-06-28 03:00:00",
				"ts_after":  "2022-06-28 05:00:00",
			}, []byte("this is body")),
		},
		{
			name: "unix to unix_ms with 30m offset",
			fields: fields{
				key: "ts",
				extra: &timestampOffsetExtra{
					FromLayout:     "unix",
					ToLayout:       "unix_ms",
					ToLocation:     "UTC",
					BeforeKey:      "ts_before_ms",
					AfterKey:       "ts_after_ms",
					OffsetDuration: "30m",
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"ts": "1656386675",
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"ts":           int64(1656386675000),
				"ts_before_ms": int64(1656386675000 - 30*60*1000),
				"ts_after_ms":  int64(1656386675000 + 30*60*1000),
			}, []byte("this is body")),
		},
		{
			name: "string to RFC3339 with 5m offset",
			fields: fields{
				key: "ts",
				extra: &timestampOffsetExtra{
					FromLayout:     "2006-01-02 15:04:05",
					FromLocation:   "Asia/Shanghai",
					ToLayout:       "2006-01-02T15:04:05Z07:00",
					ToLocation:     "Asia/Shanghai",
					BeforeKey:      "ts_before",
					AfterKey:       "ts_after",
					OffsetDuration: "5m",
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"ts": "2022-06-28 11:24:35",
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"ts":        "2022-06-28T11:24:35+08:00",
				"ts_before": "2022-06-28T11:19:35+08:00",
				"ts_after":  "2022-06-28T11:29:35+08:00",
			}, []byte("this is body")),
		},
		{
			name: "string to string with offsetToLayout unix_ms",
			fields: fields{
				key: "ts",
				extra: &timestampOffsetExtra{
					FromLayout:       "2006-01-02 15:04:05",
					FromLocation:     "Asia/Shanghai",
					ToLayout:         "2006-01-02 15:04:05",
					ToLocation:       "Asia/Shanghai",
					BeforeKey:        "ts_before",
					AfterKey:         "ts_after",
					OffsetDuration:   "5m",
					OffsetToLayout:   "unix_ms",
					OffsetToLocation: "UTC",
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"ts": "2022-06-28 11:24:35",
				}, []byte("this is body")),
			},
			want: event.NewEvent(map[string]interface{}{
				"ts":        "2022-06-28 11:24:35",
				"ts_before": int64(1656386675000 - 5*60*1000),
				"ts_after":  int64(1656386675000 + 5*60*1000),
			}, []byte("this is body")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &TimestampOffset{
				key:   tt.fields.key,
				extra: tt.fields.extra,
			}
			err := tr.act(tt.args.e)
			if err != nil {
				t.Errorf("act() error = %v", err)
				return
			}
			assertions.Equal(tt.want.Header(), tt.args.e.Header(), "header mismatch")
			assertions.Equal(tt.want.Body(), tt.args.e.Body(), "body mismatch")
		})
	}
}

func TestNewTimestampOffset_InvalidOffset(t *testing.T) {
	_, err := NewTimestampOffset([]string{"ts"}, map[string]interface{}{
		"fromLayout":     "unix_ms",
		"toLayout":       "unix",
		"beforeKey":      "ts_before",
		"afterKey":       "ts_after",
		"offsetDuration": "invalid",
	})
	if err == nil {
		t.Error("expected error for invalid offsetDuration, got nil")
	}
}

func TestNewTimestampOffset_InvalidArgs(t *testing.T) {
	_, err := NewTimestampOffset([]string{}, map[string]interface{}{
		"fromLayout":     "unix_ms",
		"toLayout":       "unix",
		"beforeKey":      "ts_before",
		"afterKey":       "ts_after",
		"offsetDuration": "5m",
	})
	if err == nil {
		t.Error("expected error for empty args, got nil")
	}
}

func TestNewTimestampOffset_MissingRequired(t *testing.T) {
	_, err := NewTimestampOffset([]string{"ts"}, map[string]interface{}{
		"fromLayout": "unix_ms",
		"toLayout":   "unix",
		// missing beforeKey, afterKey, offsetDuration
	})
	if err == nil {
		t.Error("expected validation error for missing required fields, got nil")
	}
}
