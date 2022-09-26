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

package dev

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
)

const Type = "dev"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Sink struct {
	name   string
	config *Config
	codec  codec.Codec
}

func NewSink() *Sink {
	return &Sink{
		config: &Config{},
	}
}

func (s *Sink) Category() api.Category {
	return api.SINK
}

func (s *Sink) Type() api.Type {
	return Type
}

func (s *Sink) Config() interface{} {
	return s.config
}

func (s *Sink) SetCodec(c codec.Codec) {
	s.codec = c
}

func (s *Sink) String() string {
	return fmt.Sprintf("%s/%s", api.SINK, Type)
}

func (s *Sink) Init(context api.Context) error {
	s.name = context.Name()
	return nil
}

func (s *Sink) Start() error {
	log.Info("%s start", s.String())
	return nil
}

func (s *Sink) Stop() {
}

func (s *Sink) Consume(batch api.Batch, pool api.FlowDataPool) api.Result {
	events := batch.Events()
	l := len(events)
	if l == 0 {
		return nil
	}

	if !s.config.PrintEvents {
		return result.NewResult(api.SUCCESS)
	}
	t1 := time.Now()
	for _, e := range events {
		// json encode
		_, err := s.codec.Encode(e)
		if err != nil {
			log.Warn("codec event error: %+v", err)
			continue
		}

		//log.Info("event: %s", string(out))
	}

	host := s.config.Host
	if len(host) > 0 {
		resp, err := http.Get(host)
		if err != nil {
			log.Warn(err.Error())
			pool.PutFailedResult(result.Fail(err))
			return result.Fail(err)
		}

		err = resp.Body.Close()
		if err != nil {
			log.Warn(err.Error())
		}

		t2 := time.Now()
		microseconds := t2.Sub(t1).Microseconds()
		pool.EnqueueRTT(microseconds)
	}

	if rand.Intn(10000) > 9982 {
		log.Info("put error")
		pool.PutFailedResult(result.Fail(errors.New("11")))
	}

	return result.NewResult(api.SUCCESS)
}
