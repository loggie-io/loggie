package file

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMultiFileWriter_Write(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "filesink")
	if err != nil {
		t.Errorf("creates temporary directory:%s", err)
	}
	defer os.RemoveAll(tmpDir)

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
						Filename: filepath.Join(tmpDir, "aaa.log"),
						Data:     []byte("hello world"),
					},
					{
						Filename: filepath.Join(tmpDir, "aaa.log"),
						Data:     []byte("this is aaa"),
					},
					{
						Filename: filepath.Join(tmpDir, "bbb.log"),
						Data:     []byte("hello world"),
					},
					{
						Filename: filepath.Join(tmpDir, "bbb.log"),
						Data:     []byte("this is bbb"),
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
			checkWriteResult(t, tt.args.msgs)
		})
	}
}

func checkWriteResult(t *testing.T, msgs []Message) {
	// map[filename][]data
	fc := make(map[string][][]byte)
	for _, msg := range msgs {
		fc[msg.Filename] = append(fc[msg.Filename], append(msg.Data, '\n'))
	}
	for filename, contents := range fc {
		expect := bytes.Join(contents, nil)
		actual, err := os.ReadFile(filename)
		if err != nil {
			t.Errorf("read file %s:%s", filename, err)
			continue
		}
		if !bytes.Equal(expect, actual) {
			t.Errorf("want to write:\n%s\nactually write:\n%s", expect, actual)
		}
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
