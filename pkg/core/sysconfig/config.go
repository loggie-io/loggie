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

package sysconfig

import (
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/queue"
	"github.com/loggie-io/loggie/pkg/core/reloader"
	"github.com/loggie-io/loggie/pkg/core/sink"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/discovery"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/interceptor/maxbytes"
	"github.com/loggie-io/loggie/pkg/interceptor/metric"
	"github.com/loggie-io/loggie/pkg/interceptor/retry"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/queue/channel"
	"github.com/loggie-io/loggie/pkg/util/persistence"
)

type Config struct {
	Loggie Loggie `yaml:"loggie"`
}

type Loggie struct {
	Reload           reloader.ReloadConfig       `yaml:"reload"`
	Discovery        discovery.Config            `yaml:"discovery"`
	Http             Http                        `yaml:"http" validate:"dive"`
	MonitorEventBus  eventbus.Config             `yaml:"monitor"`
	Defaults         Defaults                    `yaml:"defaults"`
	Db               persistence.DbConfig        `yaml:"db"`
	ErrorAlertConfig log.AfterErrorConfiguration `yaml:"errorAlert"`
	JSONEngine       string                      `yaml:"jsonEngine,omitempty" default:"jsoniter" validate:"oneof=jsoniter sonic std go-json"`
}

type Defaults struct {
	Sources      []*source.Config      `yaml:"sources"`
	Queue        *queue.Config         `yaml:"queue"`
	Interceptors []*interceptor.Config `yaml:"interceptors"`
	Sink         *sink.Config          `yaml:"sink"`
}

var defaultInterceptors = []*interceptor.Config{
	{
		Type: metric.Type,
	},
	{
		Type: maxbytes.Type,
	},
	{
		Type: retry.Type,
	},
}

func (d *Defaults) SetDefaults() {
	if d.Queue == nil {
		d.Queue = &queue.Config{
			Type: channel.Type,
		}
	}
	if len(d.Interceptors) == 0 {
		d.Interceptors = defaultInterceptors
	} else {
		d.Interceptors = interceptor.MergeInterceptorList(d.Interceptors, defaultInterceptors)
	}

	pipeline.SetDefaultConfigRaw(pipeline.Config{
		Sources:      d.Sources,
		Queue:        d.Queue,
		Interceptors: d.Interceptors,
		Sink:         d.Sink,
	})
}

type Http struct {
	Enabled bool   `yaml:"enabled" default:"false"`
	Host    string `yaml:"host" default:"0.0.0.0"`
	Port    int    `yaml:"port" default:"9196"`
}
