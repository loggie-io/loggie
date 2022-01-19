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

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	return &Dev{
		stop:      info.Stop,
		config:    &Config{},
		eventPool: info.EventPool,
	}
}

type Dev struct {
	name      string
	stop      bool
	eventPool *event.Pool
	config    *Config
	limiter   *rate.Limiter
	content   []byte
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

func (d *Dev) Init(context api.Context) {
	d.name = context.Name()
}

func (d *Dev) Start() {
	d.limiter = rate.NewLimiter(rate.Limit(d.config.Qps), d.config.Qps)
	d.content = make([]byte, d.config.ByteSize)
	for i := range d.content {
		d.content[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
}

func (d *Dev) Stop() {

}

func (d *Dev) Product() api.Event {
	return nil
}

func (d *Dev) ProductLoop(productFunc api.ProductFunc) {
	ctx := context.Background()
	log.Info("%s start product loop", d.String())
	content := d.content
	for !d.stop {
		header := make(map[string]interface{})
		e := d.eventPool.Get()
		e.Fill(e.Meta(), header, content)
		d.limiter.Wait(ctx)
		productFunc(e)
	}
}

func (d *Dev) Commit(events []api.Event) {
	d.eventPool.PutAll(events)
}
