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

package elasticsearch

import (
	"errors"
	"fmt"
	"github.com/loggie-io/loggie/pkg/util/pattern"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
)

const Type = "elasticsearch"

var clientNotInitError = errors.New("elasticsearch client not initialized yet")

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Sink struct {
	config *Config
	cli    *ClientSet
	codec  codec.Codec
}

func NewSink() *Sink {
	return &Sink{
		config: &Config{},
	}
}

func (s *Sink) Config() interface{} {
	return s.config
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

func (s *Sink) SetCodec(c codec.Codec) {
	s.codec = c
}

func (s *Sink) Init(context api.Context) error {
	return nil
}

func (s *Sink) Start() error {
	indexMatchers := pattern.MustInitMatcher(s.config.Index)
	cli, err := NewClient(s.config, s.codec, indexMatchers)
	if err != nil {
		log.Error("start elasticsearch connection fail, err: %v", err)
		return err
	}
	s.cli = cli
	return nil
}

func (s *Sink) Stop() {
	if s.cli != nil {
		s.cli.Stop()
	}
}

func (s *Sink) Consume(batch api.Batch) api.Result {
	if s.cli != nil {
		err := s.cli.BulkIndex(batch, s.config.Index)
		if err != nil {
			log.Error("write to elasticsearch error: %+v", err)
			return result.Fail(err)
		}

		return result.Success()
	}

	log.Error("%v", clientNotInitError)
	return result.Fail(clientNotInitError)
}
