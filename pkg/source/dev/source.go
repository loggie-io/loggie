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
	"math/rand"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/source/abstract"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"golang.org/x/time/rate"
)

const Type = "dev"

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func init() {
	abstract.SourceRegister(Type, makeSource)
}

func makeSource(info pipeline.Info) abstract.SourceConvert {
	return &Dev{
		Source: abstract.ExtendsAbstractSource(info, Type),
		stop:   info.Stop,
		config: &Config{},
	}
}

type Dev struct {
	*abstract.Source
	stop    bool
	config  *Config
	limiter *rate.Limiter
	content []byte
}

func (d *Dev) Config() interface{} {
	return d.config
}

func (d *Dev) DoStart() {
	d.limiter = rate.NewLimiter(rate.Limit(d.config.Qps), d.config.Qps)
	d.content = make([]byte, d.config.ByteSize)
	for i := range d.content {
		d.content[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
}

func (d *Dev) ProductLoop(productFunc api.ProductFunc) {
	ctx := context.Background()
	log.Info("%s start product loop", d.String())
	content := d.content
	for !d.stop {
		header := make(map[string]interface{})
		e := d.Event()
		e.Fill(e.Meta(), header, content)
		d.limiter.Wait(ctx)
		productFunc(e)
	}
}
