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
	"github.com/loggie-io/loggie/pkg/sink/codec"
)

var ErrSinkTypeRequired = errors.New("pipelines[n].sink.type is required")

type Config struct {
	cfg.ComponentBaseConfig `yaml:",inline"`
	Parallelism             int          `yaml:"parallelism,omitempty" default:"1" validate:"required,gte=1,lte=100"`
	Codec                   codec.Config `yaml:"codec" validate:"dive"`
}

func (c *Config) Validate() error {
	if c.Type == "" {
		return ErrSinkTypeRequired
	}

	return c.Codec.Validate()
}
