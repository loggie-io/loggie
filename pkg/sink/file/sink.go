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
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"github.com/loggie-io/loggie/pkg/util"
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

	filenameMatcher [][]string
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

func (s *Sink) Init(context api.Context) {
	s.filenameMatcher = util.InitMatcher(s.config.Filename)
}

func (s *Sink) Start() {
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
		msgs = append(msgs, Message{
			Filename: filename,
			Data:     e.Body(),
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
	return runtime.PatternSelect(runtime.NewObject(e.Header()), s.config.Filename, s.filenameMatcher)
}
