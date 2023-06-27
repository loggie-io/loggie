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
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"go.uber.org/atomic"
	"net/http"
	"sync"
	"time"
)

const Type = "dev"

const (
	handlerProxyPath = "/api/v1/pipeline/%s/sink/dev"

	resultStatusSuccess = "success"
	resultStatusFail    = "fail"
	resultStatusDrop    = "drop"
)

var once sync.Once

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink(info.PipelineName)
}

type Sink struct {
	name         string
	pipelineName string
	config       *Config
	codec        codec.Codec
	done         chan struct{}

	totalCount       *atomic.Uint64
	count            *atomic.Uint64
	sampleEvent      *atomic.String
	resultStatusFlag *atomic.String
}

func NewSink(pipelineName string) *Sink {
	return &Sink{
		config:       &Config{},
		count:        atomic.NewUint64(0),
		totalCount:   atomic.NewUint64(0),
		sampleEvent:  atomic.NewString(""),
		done:         make(chan struct{}),
		pipelineName: pipelineName,
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

func (s *Sink) handleHttp() {
	handlePath := fmt.Sprintf(handlerProxyPath, s.pipelineName)
	log.Info("handle http func: %+v", handlePath)
	http.HandleFunc(handlePath, s.setResultStatusHandle)
}

func (s *Sink) Init(context api.Context) error {
	s.name = context.Name()
	s.resultStatusFlag = atomic.NewString(s.config.ResultStatus)
	once.Do(func() {
		s.handleHttp()
	})
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
					qps := float64(s.count.Load()) / s.config.MetricsInterval.Seconds()
					log.Info("[dev sink] totalCount: %s, qps: %s / %s = %.1f", s.totalCount.String(), s.count.String(), s.config.MetricsInterval.String(), qps)
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

	// Simulation failed or discarded batch scenarios
	status := s.resultStatusFlag.Load()
	if status == resultStatusFail {
		return result.Fail(errors.New("mock failed"))
	} else if status == resultStatusDrop {
		return result.DropWith(errors.New("mock drop"))
	}

	if s.config.PrintMetrics {
		s.count.Add(uint64(l))
		s.totalCount.Add(uint64(l))
	}

	for i, e := range events {
		// json encode
		out, err := s.codec.Encode(e)
		if err != nil {
			log.Warn("codec event error: %+v", err)
			continue
		}

		if s.config.PrintEvents {
			if s.config.PrintEventsInterval <= 0 {
				log.Info("[event]: %s", string(out))
			} else if i == l-1 {
				s.sampleEvent.Store(string(out))
			}
		}
	}

	return result.NewResult(api.SUCCESS)
}

func (s *Sink) setResultStatusHandle(writer http.ResponseWriter, request *http.Request) {
	status := request.URL.Query().Get("status")
	if status != "" {
		if status != resultStatusSuccess && status != resultStatusFail && status != resultStatusDrop {
			writer.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(writer, "param status is invalid, it can be: success, fail, drop")
			return
		}

		s.resultStatusFlag.Store(status)
		writer.WriteHeader(http.StatusOK)
		fmt.Fprintf(writer, "set result status to: %s", status)
		return
	}
}
