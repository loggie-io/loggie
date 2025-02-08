package file

import (
	"io"
	"os"
	"testing"
	"time"
)

func TestWriter_Write(t *testing.T) {
	type fields struct {
		W             io.Writer
		Size          int
		FlushInterval time.Duration
	}
	type args struct {
		bs []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "test-1",
			fields: fields{
				W:             os.Stdout,
				Size:          1,
				FlushInterval: time.Second,
			},
			args:    args{[]byte("test\n")},
			want:    5,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Writer{
				W:             tt.fields.W,
				Size:          tt.fields.Size,
				FlushInterval: tt.fields.FlushInterval,
			}
			got, err := w.Write(tt.args.bs)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Write() got = %v, want %v", got, tt.want)
			}
			w.Stop()
		})
	}
}

func BenchmarkWriter_Write(b *testing.B) {
	w := &Writer{
		W: io.Discard,
	}
	defer w.Stop()
	bytes := []byte("hello world")
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		w.Write(bytes)
	}
}
