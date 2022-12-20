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

package sink

import (
	"errors"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/concurrency"
	"github.com/loggie-io/loggie/pkg/sink/codec"
)

var ErrSinkTypeRequired = errors.New("pipelines[n].sink.type is required")

type Config struct {
	Enabled     *bool              `yaml:"enabled,omitempty"`
	Name        string             `yaml:"name,omitempty"`
	Type        string             `yaml:"type,omitempty" validate:"required"`
	Properties  cfg.CommonCfg      `yaml:",inline"`
	Parallelism int                `yaml:"parallelism,omitempty" default:"1" validate:"required,gte=1,lte=100"`
	Codec       codec.Config       `yaml:"codec,omitempty" validate:"dive"`
	Concurrency concurrency.Config `yaml:"concurrency,omitempty"`
}

func (c *Config) Validate() error {
	if c.Type == "" {
		return ErrSinkTypeRequired
	}

	return c.Codec.Validate()
}

func (c *Config) Merge(from *Config) {
	if from == nil {
		return
	}
	if c.Name != from.Name || c.Type != from.Type {
		return
	}

	c.Properties = cfg.MergeCommonCfg(c.Properties, from.Properties, false)

	if c.Parallelism == 0 {
		c.Parallelism = from.Parallelism
	}

	if c.Codec.Type == "" {
		c.Codec = from.Codec
	} else {
		c.Codec = *c.Codec.Merge(&from.Codec)
	}
}
