/*
Copyright 2023 Loggie Authors

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

package hdfs

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"github.com/loggie-io/loggie/pkg/sink/file"
	"github.com/loggie-io/loggie/pkg/util/consistent"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"io"
	"time"
)

const Type = "hdfs"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Sink struct {
	config *Config
	writer *file.MultiFileWriter
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
	w, err := file.NewMultiFileWriter(&file.Options{
		WorkerCount: 1,
		LocalTime:   c.LocalTime,
		IdleTimeout: 5 * time.Minute,
	}, func(filename string, options *file.Options) (io.WriteCloser, error) {
		return makeClient(s.config.NameNode, filename)
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
	msgs := make([]file.Message, 0, l)
	for _, e := range events {
		filename, err := file.SelectFileName(e, s.consistent, s.dirHashKeyPattern, s.filenamePattern)
		if err != nil {
			log.Error("select filename error: %+v", err)
			return result.Fail(err)
		}
		data, err := s.cod.Encode(e)
		if err != nil {
			log.Warn("codec event error: %+v", err)
			continue
		}
		msgs = append(msgs, file.Message{
			Filename: filename,
			Data:     data,
		})
	}
	err := s.writer.Write(msgs...)
	if err != nil {
		return result.Fail(err)
	}

	return result.Success()
}
