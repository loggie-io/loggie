package eventops

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGet(t *testing.T) {
	assertions := assert.New(t)

	type args struct {
		e   api.Event
		key string
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "get a string",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
						"c": nil,
					},
				}, []byte("this is body")),
				key: "a.b",
			},
			want: "foo",
		},
		{
			name: "get nil",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
						"c": nil,
					},
				}, []byte("this is body")),
				key: "a.c",
			},
			want: nil,
		},
		{
			name: "get field not exist",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
						"c": nil,
					},
				}, []byte("this is body")),
				key: "a.d",
			},
			want: nil,
		},
		{
			name: "get body bytes",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
						"c": nil,
					},
				}, []byte("this is body")),
				key: "body",
			},
			want: []byte("this is body"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Get(tt.args.e, tt.args.key)
			assertions.Equal(tt.want, got)
		})
	}
}

func TestSet(t *testing.T) {
	assertions := assert.New(t)

	type args struct {
		e   api.Event
		key string
		val interface{}
	}
	tests := []struct {
		name string
		args args
		want api.Event
	}{
		{
			name: "ok",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{},
				}, []byte("this is body")),
				key: "a.b",
				val: "foo",
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "foo",
				},
			}, []byte("this is body")),
		},
		{
			name: "change the field type",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
					},
				}, []byte("this is body")),
				key: "a.b",
				val: map[string]interface{}{
					"c": "d",
				},
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": map[string]interface{}{
						"c": "d",
					},
				},
			}, []byte("this is body")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Set(tt.args.e, tt.args.key, tt.args.val)
			assertions.Equal(tt.want, tt.args.e)
		})
	}
}

func TestCopy(t *testing.T) {
	assertions := assert.New(t)

	type args struct {
		e    api.Event
		from string
		to   string
	}
	tests := []struct {
		name string
		args args
		want api.Event
	}{
		{
			name: "copy a.b to c.d",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
					},
				}, []byte("this is body")),
				from: "a.b",
				to:   "c.d",
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "foo",
				},
				"c": map[string]interface{}{
					"d": "foo",
				},
			}, []byte("this is body")),
		},
		{
			name: "copy a.b to c",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
					},
				}, []byte("this is body")),
				from: "a.b",
				to:   "c",
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "foo",
				},
				"c": "foo",
			}, []byte("this is body")),
		},
		{
			name: "copy a.b to c, but c is exist, so c would be override",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
					},
					"c": map[string]interface{}{
						"g": "h",
					},
				}, []byte("this is body")),
				from: "a.b",
				to:   "c",
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "foo",
				},
				"c": "foo",
			}, []byte("this is body")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Copy(tt.args.e, tt.args.from, tt.args.to)
			assertions.Equal(tt.want, tt.args.e)
		})
	}
}

func TestDel(t *testing.T) {
	assertions := assert.New(t)

	type args struct {
		e   api.Event
		key string
	}
	tests := []struct {
		name string
		args args
		want api.Event
	}{
		{
			name: "ok",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
					},
				}, []byte("this is body")),
				key: "a.b",
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{},
			}, []byte("this is body")),
		},
		{
			name: "delete value is nil",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": nil,
					},
				}, []byte("this is body")),
				key: "a.b",
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{},
			}, []byte("this is body")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Del(tt.args.e, tt.args.key)
			assertions.Equal(tt.want, tt.args.e)
		})
	}
}

func TestDelKeys(t *testing.T) {
	assertions := assert.New(t)

	type args struct {
		e    api.Event
		keys []string
	}
	tests := []struct {
		name string
		args args
		want api.Event
	}{
		{
			name: "ok",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
					},
					"c": "d",
				}, []byte("this is body")),
				keys: []string{"a.b", "c"},
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{},
			}, []byte("this is body")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			DelKeys(tt.args.e, tt.args.keys)
			assertions.Equal(tt.want, tt.args.e)
		})
	}
}

func TestMove(t *testing.T) {
	assertions := assert.New(t)

	type args struct {
		e    api.Event
		from string
		to   string
	}
	tests := []struct {
		name string
		args args
		want api.Event
	}{
		{
			name: "ok",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
					},
				}, []byte("this is body")),
				from: "a.b",
				to:   "c.d",
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{},
				"c": map[string]interface{}{
					"d": "foo",
				},
			}, []byte("this is body")),
		},
		{
			name: "rename body",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
					},
				}, []byte("this is body")),
				from: event.Body,
				to:   "log",
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "foo",
				},
				"log": "this is body",
			}, []byte{}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Move(tt.args.e, tt.args.from, tt.args.to)
			assertions.Equal(tt.want, tt.args.e)
		})
	}
}

func TestUnderRoot(t *testing.T) {
	assertions := assert.New(t)

	type args struct {
		e   api.Event
		key string
	}
	tests := []struct {
		name string
		args args
		want api.Event
	}{
		{
			name: "ok",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
						"c": "bar",
					},
				}, []byte("this is body")),
				key: "a",
			},
			want: event.NewEvent(map[string]interface{}{
				"b": "foo",
				"c": "bar",
			}, []byte("this is body")),
		},
		{
			name: "key not exist",
			args: args{
				e: event.NewEvent(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "foo",
						"c": "bar",
					},
				}, []byte("this is body")),
				key: "a.g",
			},
			want: event.NewEvent(map[string]interface{}{
				"a": map[string]interface{}{
					"b": "foo",
					"c": "bar",
				},
				"g": nil,
			}, []byte("this is body")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			UnderRoot(tt.args.e, tt.args.key)
			assertions.Equal(tt.want, tt.args.e)
		})
	}
}
