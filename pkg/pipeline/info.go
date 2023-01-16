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

package pipeline

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/core/queue"
	"github.com/loggie-io/loggie/pkg/core/sink"
	"github.com/loggie-io/loggie/pkg/core/source"
)

type Config struct {
	Name             string        `yaml:"name,omitempty" validate:"required"`
	CleanDataTimeout time.Duration `yaml:"cleanDataTimeout,omitempty" default:"5s"`

	Queue        *queue.Config         `yaml:"queue,omitempty" validate:"dive,required"`
	Interceptors []*interceptor.Config `yaml:"interceptors,omitempty"`
	Sources      []*source.Config      `yaml:"sources,omitempty" validate:"dive,required"`
	Sink         *sink.Config          `yaml:"sink,omitempty" validate:"dive,required"`
}

func (c *Config) SetDefaults() {
	c.merge()
}

func (c *Config) Validate() error {
	return NewPipeline(c).validate()
}

func (c *Config) DeepCopy() *Config {
	if c == nil {
		return nil
	}

	out := new(Config)
	out.Name = c.Name
	out.CleanDataTimeout = c.CleanDataTimeout
	out.Queue = c.Queue.DeepCopy()

	if len(c.Interceptors) > 0 {
		icp := make([]*interceptor.Config, 0)
		for _, conf := range c.Interceptors {
			icp = append(icp, conf.DeepCopy())
		}
		out.Interceptors = icp
	}

	if len(c.Sources) > 0 {
		src := make([]*source.Config, 0)
		for _, conf := range c.Sources {
			src = append(src, conf.DeepCopy())
		}
		out.Sources = src
	}

	out.Sink = c.Sink.DeepCopy()

	return out
}

func (c *Config) merge() {
	defaults, err := GetDefaultConfigRaw()
	if err != nil {
		log.Fatal("get default pipeline config error: %v", err)
	}

	if c.Queue != nil {
		c.Queue.Merge(defaults.Queue)
	} else {
		c.Queue = defaults.Queue
	}

	if len(c.Interceptors) != 0 {
		c.Interceptors = interceptor.MergeInterceptorList(c.Interceptors, defaults.Interceptors)
	} else {
		c.Interceptors = defaults.Interceptors
	}

	if c.Sink != nil {
		c.Sink.Merge(defaults.Sink)
	} else {
		c.Sink = defaults.Sink
	}

	if len(c.Sources) != 0 {
		c.Sources = source.MergeSourceList(c.Sources, defaults.Sources)
	} else {
		c.Sources = defaults.Sources
	}
}

type Info struct {
	Stop         bool // lazy stop signal
	PipelineName string
	SurviveChan  chan api.Batch
	Epoch        *Epoch
	R            *RegisterCenter
	SinkCount    int
	EventPool    *event.Pool
}
