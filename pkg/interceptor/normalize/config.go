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

package normalize

import (
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
)

// Config Deprecated, use transformer instead
type Config struct {
	interceptor.ExtensionConfig `yaml:",inline"`
	Processors                  ProcessorConfig `yaml:"processors,omitempty"`
}

type ProcessorConfig []map[string]cfg.CommonCfg

type Convert struct {
	From string `yaml:"from,omitempty" validate:"required"`
	To   string `yaml:"to,omitempty" validate:"required"`
}

func (c *Config) Validate() error {
	for _, proc := range c.Processors {
		for k, v := range proc {
			_, err := newProcessor(k, v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
