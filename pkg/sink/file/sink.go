/*
Copyright 2021 Loggie Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package file

import (
	"fmt"
	"strings"
	"time"

	"github.com/loggie-io/loggie/pkg/util/pattern"

	"github.com/loggie-io/loggie/pkg/util/consistent"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"github.com/loggie-io/loggie/pkg/util/runtime"
)

const Type = "file"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Sink struct {
	config *Config
	writer *MultiFileWriter
	cod    codec.Codec

	consistent *consistent.Consistent

	dirHashKeyPattern *pattern.Pattern
	filenamePattern   *pattern.Pattern
}

func NewSink() *Sink {
	return &Sink{
		config: &Config{},
	}
}

func (s *Sink) Config() interface{} {
	return s.config
}

func (s *Sink) SetCodec(c codec.Codec) {
	s.cod = c
}

func (s *Sink) Category() api.Category {
	return api.SINK
}

func (s *Sink) Type() api.Type {
	return Type
}

func (s *Sink) String() string {
	return fmt.Sprintf("%s/%s", api.SINK, Type)
}

func (s *Sink) Init(context api.Context) error {
	if len(s.config.BaseDirs) > 0 || len(s.config.DirHashKey) > 0 {
		s.consistent = consistent.New()
		for i := range s.config.BaseDirs {
			s.consistent.Add(s.config.BaseDirs[i])
		}
	}

	s.dirHashKeyPattern, _ = pattern.Init(s.config.DirHashKey)
	s.filenamePattern, _ = pattern.Init(s.config.Filename)
	return nil
}

func (s *Sink) Start() error {
	c := s.config
	w, err := NewMultiFileWriter(&Options{
		WorkerCount: c.WorkerCount,
		MaxSize:     c.MaxSize,
		MaxAge:      c.MaxAge,
		MaxBackups:  c.MaxBackups,
		LocalTime:   c.LocalTime,
		Compress:    c.Compress,
		IdleTimeout: 5 * time.Minute,
	})
	if err != nil {
		log.Panic("start multi file writer failed, error: %v", err)
	}

	s.writer = w

	log.Info("file-sink start,filename: %s", s.config.Filename)
	return nil
}

func (s *Sink) Stop() {
	if s.writer != nil {
		_ = s.writer.Close()
	}
}

func (s *Sink) Consume(batch api.Batch) api.Result {
	events := batch.Events()
	l := len(events)
	if l == 0 {
		return result.Success()
	}
	msgs := make([]Message, 0, l)
	for _, e := range events {
		filename, err := s.selectFilename(e)
		if err != nil {
			log.Error("select filename error: %+v", err)
			return result.Fail(err)
		}
		data, err := s.cod.Encode(e)
		if err != nil {
			log.Warn("codec event error: %+v", err)
			continue
		}
		msgs = append(msgs, Message{
			Filename: filename,
			Data:     data,
		})
	}
	err := s.writer.Write(msgs...)
	if err != nil {
		log.Error("write to file error: %v", err)
		return result.Fail(err)
	}
	return result.Success()
}

func (s *Sink) selectFilename(e api.Event) (string, error) {
	var dir string
	headerObj := runtime.NewObject(e.Header())
	if s.consistent != nil {
		dirHashKey, err := s.dirHashKeyPattern.WithObject(headerObj).Render()
		if err != nil {
			return "", err
		}
		dir, err = s.consistent.Get(dirHashKey)
		if err != nil {
			return "", err
		}
	}
	filename, err := s.filenamePattern.WithObject(headerObj).Render()
	if err != nil {
		return "", err
	}
	if len(dir) == 0 {
		return filename, nil
	}
	var sb strings.Builder
	sb.WriteString(dir)
	sb.WriteString(filename)
	return sb.String(), nil
}
