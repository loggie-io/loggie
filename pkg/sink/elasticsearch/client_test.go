package elasticsearch

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClientSet_writeMeta(t *testing.T) {
	type fields struct {
		buf *bytes.Buffer
		aux []byte
	}
	type args struct {
		action     string
		documentID string
		index      string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "ok",
			fields: fields{
				buf: bytes.NewBuffer(make([]byte, 0, 512)),
				aux: make([]byte, 0, 512),
			},
			args: args{
				action:     "index",
				documentID: "",
				index:      "test",
			},
			want: fmt.Sprintf("%s\n", `{"index":{"_index":"test"}}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClientSet{
				buf: tt.fields.buf,
				aux: tt.fields.aux,
			}
			err := c.writeMeta(tt.args.action, tt.args.documentID, tt.args.index)
			assert.NoErrorf(t, err, "write bulk meta")
			assert.Equal(t, tt.want, c.buf.String())
			c.buf.Reset()
		})
	}
}
