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
	"loggie.io/loggie/pkg/core/cfg"
	"loggie.io/loggie/pkg/core/reloader"
	"loggie.io/loggie/pkg/discovery"
	"loggie.io/loggie/pkg/eventbus"
	"loggie.io/loggie/pkg/interceptor/maxbytes"
	"loggie.io/loggie/pkg/interceptor/metric"
	"loggie.io/loggie/pkg/interceptor/retry"
	"loggie.io/loggie/pkg/queue/channel"
)

type Config struct {
	Loggie Loggie `yaml:"loggie"`
}

type Loggie struct {
	Reload          reloader.ReloadConfig `yaml:"reload"`
	Discovery       discovery.Config      `yaml:"discovery"`
	Http            Http                  `yaml:"http" validate:"dive"`
	MonitorEventBus eventbus.Config       `yaml:"monitor"`
	Defaults        Defaults              `yaml:"defaults"`
}

type Defaults struct {
	Sources      []cfg.CommonCfg `yaml:"sources"`
	Queue        cfg.CommonCfg   `yaml:"queue"`
	Interceptors []cfg.CommonCfg `yaml:"interceptors"`
	Sinks        cfg.CommonCfg   `yaml:"sink"`
}

func (d *Defaults) SetDefaults() {
	if d.Queue == nil {
		d.Queue = cfg.CommonCfg{
			"type": channel.Type,
			"name": "default",
		}
	}
	if d.Interceptors == nil || len(d.Interceptors) == 0 {
		d.Interceptors = []cfg.CommonCfg{
			{
				"type": metric.Type,
			},
			{
				"type": maxbytes.Type,
			},
			{
				"type": retry.Type,
			},
		}
	}
}

type Http struct {
	Enabled bool   `yaml:"enabled" default:"false"`
	Host    string `yaml:"host" default:"0.0.0.0"`
	Port    int    `yaml:"port" default:"9196"`
}
