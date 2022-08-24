/*
Copyright 2022 Loggie Authors

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

package codec

import (
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/pkg/errors"
)

type Config struct {
	Type          string `yaml:"type,omitempty" default:"json"`
	cfg.CommonCfg `yaml:",inline"`
}

func (c *Config) DeepCopy() *Config {
	if c == nil {
		return nil
	}
	out := &Config{
		Type:      c.Type,
		CommonCfg: c.CommonCfg.DeepCopy(),
	}
	return out
}

func (c *Config) Validate() error {
	if c.Type != "json" && c.Type != "regex" {
		return errors.Errorf("codec %s is not supported", c.Type)
	}
	return nil
}

func (c *Config) Merge(from *Config) {
	if from == nil {
		return
	}
	if c.Type != from.Type {
		return
	}

	c.CommonCfg = cfg.MergeCommonCfg(c.CommonCfg, from.CommonCfg, false)
}
