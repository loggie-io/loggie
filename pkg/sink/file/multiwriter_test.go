package file

import (
	"testing"
	"time"
)

func TestMultiFileWriter_Write(t *testing.T) {
	type fields struct {
		opt *Options
	}
	type args struct {
		msgs []Message
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "test-1",
			fields: fields{
				opt: &Options{
					WorkerCount: 3,
					MaxSize:     1,
					MaxAge:      1,
					MaxBackups:  1,
					Compress:    false,
					IdleTimeout: 30 * time.Second,
				},
			},
			args: args{
				msgs: []Message{
					{
						Filename: "/tmp/filesink/aaa.log",
						Data:     []byte("hello world"),
					},
					{
						Filename: "/tmp/filesink/bbb.log",
						Data:     []byte("hello world"),
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := NewMultiFileWriter(tt.fields.opt)
			if err != nil {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := w.Write(tt.args.msgs...); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			w.Close()
		})
	}
}

func BenchmarkMultiFileWriter_Write(b *testing.B) {
	w, _ := NewMultiFileWriter(&Options{
		WorkerCount: 3,
		MaxSize:     1,
		MaxAge:      1,
		MaxBackups:  1,
		Compress:    false,
		IdleTimeout: 30 * time.Second,
	})
	defer w.Close()
	msgs := []Message{
		{
			Filename: "/tmp/filesink/aaa.log",
			Data:     []byte("hello world"),
		},
		{
			Filename: "/tmp/filesink/bbb.log",
			Data:     []byte("hello world"),
		},
	}
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		w.Write(msgs...)
	}
}
