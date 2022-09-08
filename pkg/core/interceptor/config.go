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

package interceptor

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/cfg"
)

type Config struct {
	Enabled    *bool         `yaml:"enabled,omitempty"`
	Name       string        `yaml:"name,omitempty"`
	Type       string        `yaml:"type,omitempty" validate:"required"`
	Properties cfg.CommonCfg `yaml:",inline"`
}

func (c *Config) GetExtension() (*ExtensionConfig, error) {
	ext := &ExtensionConfig{}

	if err := cfg.UnpackFromCommonCfg(c.Properties, ext).Do(); err != nil {
		return nil, err
	}
	return ext, nil
}

func (c *Config) SetBelongTo(belongTo []string) {
	c.Properties["belongTo"] = belongTo
}

func (c *Config) UID() string {
	return fmt.Sprintf("%s/%s", c.Type, c.Name)
}

func (c *Config) Merge(from *Config) {
	if c.Name != from.Name || c.Type != from.Type {
		return
	}

	c.Properties = cfg.MergeCommonCfg(c.Properties, from.Properties, false)
}

// MergeInterceptorList merge interceptor lists with defaults.
// The interceptor will distinguish whether to merge or not according to the UID() by name and type.
func MergeInterceptorList(base []*Config, from []*Config) []*Config {
	if len(base) == 0 {
		return from
	}
	if len(from) == 0 {
		return base
	}

	fromMap := make(map[string]*Config)
	for _, v := range from {
		fromMap[v.UID()] = v
	}

	for _, baseCfg := range base {
		baseUID := baseCfg.UID()
		fromCfg, ok := fromMap[baseUID]
		if ok {
			baseCfg.Merge(fromCfg)
			delete(fromMap, baseUID)
			continue
		}
	}

	// The map is unordered, and the order of `from config` needs to be guaranteed here.
	for _, f := range from {
		if _, ok := fromMap[f.UID()]; ok {
			base = append(base, f)
		}
	}

	return base
}
