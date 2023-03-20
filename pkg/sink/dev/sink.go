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
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"go.uber.org/atomic"
	"time"
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
	done   chan struct{}

	count       *atomic.Uint64
	sampleEvent *atomic.String
}

func NewSink() *Sink {
	return &Sink{
		config:      &Config{},
		count:       atomic.NewUint64(0),
		sampleEvent: atomic.NewString(""),
		done:        make(chan struct{}),
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

	if s.config.PrintMetrics {
		go func() {
			tick := time.NewTicker(s.config.MetricsInterval)
			defer tick.Stop()
			for {
				select {
				case <-s.done:
					return

				case <-tick.C:
					// print metrics logs
					log.Info("[dev sink] qps: %s / %s", s.count.String(), s.config.MetricsInterval.String())
					s.count.Store(0)
				}
			}
		}()
	}

	if s.config.PrintEventsInterval > 0 {
		go func() {
			tick := time.NewTicker(s.config.PrintEventsInterval)
			defer tick.Stop()
			for {
				select {
				case <-s.done:
					return

				case <-tick.C:
					// print sample log event
					event := s.sampleEvent.Load()
					if event != "" {
						log.Info("[event]: %s", event)
					}
				}
			}
		}()
	}

	return nil
}

func (s *Sink) Stop() {
	close(s.done)
}

func (s *Sink) Consume(batch api.Batch) api.Result {
	events := batch.Events()
	l := len(events)
	if l == 0 {
		return result.Success()
	}

	if s.config.PrintMetrics {
		s.count.Add(uint64(l))
	}

	if s.config.PrintEvents {
		// print every events
		if s.config.PrintEventsInterval == 0 {
			for _, e := range events {
				// json encode
				out, err := s.codec.Encode(e)
				if err != nil {
					log.Warn("codec event error: %+v", err)
					continue
				}

				e := string(out)
				log.Info("[event]: %s", e)
			}
		}

		// print sample events
		if s.config.PrintEventsInterval > 0 {
			e := events[0]
			out, err := s.codec.Encode(e)
			if err != nil {
				log.Warn("codec event error: %+v", err)
			}

			s.sampleEvent.Store(string(out))
		}
	}

	return result.NewResult(api.SUCCESS)
}
