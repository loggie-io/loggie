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
	"time"

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

	if len(d.config.Contents) > 0 {
		GenLinesFromContent(d.stop, d.config.EventsTotal, d.config.Qps, d.config.Interval, d.config.Contents, func(content []byte, header map[string]interface{}, index int64) {
			e := d.eventPool.Get()
			e.Fill(e.Meta(), header, content)
			productFunc(e)
		})
		return
	}

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

// GenLinesFromContent generates events from static content configuration.
// It cycles through the provided contents, using the interval as the delay between events.
// The "body" key in content is treated as the event body, other keys are set as header fields.
func GenLinesFromContent(stop chan struct{}, totalCount int64, qps int, interval time.Duration, contents []ContentConfig, outFunc func(content []byte, header map[string]interface{}, index int64)) {
	var index int64
	contentLen := int64(len(contents))

	// Use interval if provided, otherwise fallback to qps-based rate limiting
	var limiter *rate.Limiter
	var ticker *time.Ticker
	if interval > 0 {
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	} else if qps > 0 {
		limiter = rate.NewLimiter(rate.Limit(qps), qps)
	}

	for {
		select {
		case <-stop:
			return

		default:
		}

		if ticker != nil {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		} else if limiter != nil {
			if err := limiter.Wait(context.TODO()); err != nil {
				return
			}
		}

		idx := index % contentLen
		item := contents[idx]

		// Extract body and header fields from the content item
		var body []byte
		header := make(map[string]interface{})
		for k, v := range item {
			if k == "body" {
				if s, ok := v.(string); ok {
					body = []byte(s)
				}
			} else {
				header[k] = v
			}
		}

		outFunc(body, header, index)

		index++

		if totalCount > 0 && index >= totalCount {
			return
		}
	}
}
