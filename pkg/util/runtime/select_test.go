package runtime

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetQueryPaths(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "normal",
			args: args{
				query: `a.b`,
			},
			want: []string{"a", "b"},
		},
		{
			name: "a.b.c",
			args: args{
				query: `a.b.c`,
			},
			want: []string{"a", "b", "c"},
		},
		{
			name: "[a.b].c",
			args: args{
				query: `[a.b].c`,
			},
			want: []string{"a.b", "c"},
		},
		{
			name: "a.[b.c]",
			args: args{
				query: `a.[b.c]`,
			},
			want: []string{"a", "b.c"},
		},
		{
			name: "a.[b.c].d",
			args: args{
				query: `a.[b.c].d`,
			},
			want: []string{"a", "b.c", "d"},
		},
		{
			name: "[a.b].c.[d.e]",
			args: args{
				query: `[a.b].c.[d.e]`,
			},
			want: []string{"a.b", "c", "d.e"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetQueryPaths(tt.args.query)
			assert.Equal(t, got, tt.want)
		})
	}
}

func TestGetQueryUpperPaths(t *testing.T) {
	type args struct {
		query string
	}
	type want struct {
		before []string
		last   string
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "normal",
			args: args{
				query: "a.b.c",
			},
			want: want{
				before: []string{"a", "b"},
				last:   "c",
			},
		},
		{
			name: "[a.b].c",
			args: args{
				query: "[a.b].c",
			},
			want: want{
				before: []string{"a.b"},
				last:   "c",
			},
		},
		{
			name: "a.[b.c]",
			args: args{
				query: "a.[b.c].d",
			},
			want: want{
				before: []string{"a", "b.c"},
				last:   "d",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before, last := GetQueryUpperPaths(tt.args.query)
			assert.Equal(t, before, tt.want.before)
			assert.Equal(t, last, tt.want.last)
		})
	}
}
