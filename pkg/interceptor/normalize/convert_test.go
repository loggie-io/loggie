package normalize

import (
	"reflect"
	"testing"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
)

func TestFormatProcessor_Process(t *testing.T) {
	type fields struct {
		config *ConvertConfig
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
			fields: fields{config: &ConvertConfig{
				Convert: []Convert{
					{
						From: "a",
						To:   typeBoolean,
					},
					{
						From: "b",
						To:   typeInteger,
					},
					{
						From: "c",
						To:   typeFloat,
					},
					{
						From: "e.f",
						To:   typeFloat,
					},
				},
			}},
			args: args{e: &event.DefaultEvent{
				H: map[string]interface{}{
					"a": "t",
					"b": "123",
					"c": "234.0",
					"d": "e",
					"e": map[string]interface{}{
						"f": "345.678",
						"g": "other",
					},
				},
				B: nil,
			}},
			want: map[string]interface{}{
				"a": true,
				"b": int64(123),
				"c": 234.0,
				"d": "e",
				"e": map[string]interface{}{
					"f": 345.678,
					"g": "other",
				},
			},
		},
		{
			name: "invalid path value",
			fields: fields{config: &ConvertConfig{
				Convert: []Convert{
					{
						From: "a",
						To:   typeBoolean,
					},
					{
						From: "b",
						To:   typeInteger,
					},
					{
						From: "c",
						To:   typeFloat,
					},
					{
						From: "e.f",
						To:   typeFloat,
					},
				},
			}},
			args: args{e: &event.DefaultEvent{
				H: map[string]interface{}{
					"a": "t",
					"b": "123",
					"c": "234.0",
					"d": "e",
					"e": map[string]interface{}{
						"f": struct{}{},
						"g": "other",
					},
				},
				M: event.NewDefaultMeta(),
				B: nil,
			}},
			want: map[string]interface{}{
				"a": true,
				"b": int64(123),
				"c": 234.0,
				"d": "e",
				"e": map[string]interface{}{
					"f": struct{}{},
					"g": "other",
				},
			},
		},
		{
			name:   "nil format config",
			fields: fields{config: &ConvertConfig{}},
			args: args{e: &event.DefaultEvent{
				H: map[string]interface{}{
					"a": "t",
					"b": "123",
					"c": "234.0",
					"d": "e",
				},
				B: nil,
			}},
			want: map[string]interface{}{
				"a": "t",
				"b": "123",
				"c": "234.0",
				"d": "e",
			},
		},
		{
			name: "mismatch",
			fields: fields{config: &ConvertConfig{
				Convert: []Convert{
					{
						From: "e",
						To:   typeBoolean,
					},
					{
						From: "f",
						To:   typeInteger,
					},
					{
						From: "g",
						To:   typeFloat,
					},
				},
			}},
			args: args{e: &event.DefaultEvent{
				H: map[string]interface{}{
					"a": "t",
					"b": "123",
					"c": "234.0",
					"d": "e",
				},
				B: nil,
			}},
			want: map[string]interface{}{
				"a": "t",
				"b": "123",
				"c": "234.0",
				"d": "e",
			},
		},
	}

	log.InitDefaultLogger()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ConvertProcessor{
				config: tt.fields.config,
			}
			_ = p.Process(tt.args.e)
			if !reflect.DeepEqual(tt.want, tt.args.e.Header()) {
				t.Errorf("Process() got = %v, want=%v", tt.args.e.Header(), tt.want)
			}
		})
	}
}
