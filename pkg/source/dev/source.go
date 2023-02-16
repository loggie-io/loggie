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
	"context"
	"fmt"
	"math/rand"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"golang.org/x/time/rate"
)

const Type = "dev"

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ "

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	return &Dev{
		stop:      make(chan struct{}),
		config:    &Config{},
		eventPool: info.EventPool,
	}
}

type Dev struct {
	name      string
	stop      chan struct{}
	eventPool *event.Pool
	config    *Config
}

func (d *Dev) Config() interface{} {
	return d.config
}

func (d *Dev) Category() api.Category {
	return api.SOURCE
}

func (d *Dev) Type() api.Type {
	return Type
}

func (d *Dev) String() string {
	return fmt.Sprintf("%s/%s", api.SOURCE, Type)
}

func (d *Dev) Init(context api.Context) error {
	d.name = context.Name()
	return nil
}

func (d *Dev) Start() error {
	return nil
}

func (d *Dev) Stop() {
	close(d.stop)
}

func (d *Dev) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", d.String())

	GenLines(d.stop, d.config.EventsTotal, d.config.ByteSize, d.config.Qps, func(content []byte, index int64) {
		header := make(map[string]interface{})

		e := d.eventPool.Get()
		e.Fill(e.Meta(), header, content)
		productFunc(e)
	})
}

func (d *Dev) Commit(events []api.Event) {
	d.eventPool.PutAll(events)
}

func GenLines(stop chan struct{}, totalCount int64, lineBytes int, qps int, outFunc func(content []byte, index int64)) {
	total := totalCount
	var index int64
	content := make([]byte, lineBytes)
	limiter := rate.NewLimiter(rate.Limit(qps), qps)

	for {
		select {
		case <-stop:
			return

		default:
			for i := range content {
				content[i] = letterBytes[rand.Intn(len(letterBytes))]
			}

			limiter.Wait(context.TODO())
			index++

			if total > 0 {
				total--
			} else if total == 0 {
				return
			} // total <= 0: continue, make infinite events

			outFunc(content, index)
		}
	}
}
