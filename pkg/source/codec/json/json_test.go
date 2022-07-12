package json

import (
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
