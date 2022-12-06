/*
Copyright 2022 Loggie Authors

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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

const Type = "elasticsearch"

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	return &Source{
		pipelineName: info.PipelineName,
		done:         make(chan struct{}),
		config:       &Config{},
		eventPool:    info.EventPool,
	}
}

type Source struct {
	pipelineName string
	name         string
	done         chan struct{}
	closeOnce    sync.Once
	config       *Config
	cli          *ClientSet
	eventPool    *event.Pool
}

func (s *Source) Config() interface{} {
	return s.config
}

func (s *Source) Category() api.Category {
	return api.SOURCE
}

func (s *Source) Type() api.Type {
	return Type
}

func (s *Source) String() string {
	return fmt.Sprintf("%s/%s", api.SOURCE, Type)
}

func (s *Source) Init(context api.Context) error {
	s.name = context.Name()
	return nil
}

func (s *Source) Stop() {
	s.closeOnce.Do(func() {
		if s.cli != nil {
			s.cli.Stop()
		}
		log.Info("stopping source %s: %s", Type, s.name)
		close(s.done)
	})
}

func (s *Source) Start() error {
	s.config.PipelineName = s.pipelineName
	s.config.Name = s.name
	cli, err := NewClient(s.config)
	if err != nil {
		log.Error("start elasticsearch connection fail, err: %v", err)
		return err
	}
	s.cli = cli

	return nil
}

func (s *Source) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", s.String())

	if s.cli == nil {
		log.Error("source elasticsearch client not initialized yet")
		return
	}
	t := time.NewTicker(s.config.Interval)
	defer t.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case <-s.done:
			return

		case <-t.C:
			s.batchScrape(ctx, productFunc)
		}
	}
}

func (s *Source) batchScrape(ctx context.Context, productFunc api.ProductFunc) {
	ct, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	datas, err := s.cli.Search(ct)
	if err != nil {
		log.Warn("request to elasticsearch error: %+v", err)
		return
	}

	for i := 0; i < len(datas); i++ {
		e := s.eventPool.Get()
		e.Fill(e.Meta(), e.Header(), datas[i])
		productFunc(e)
	}
}

func (s *Source) Commit(events []api.Event) {
	s.eventPool.PutAll(events)
}
