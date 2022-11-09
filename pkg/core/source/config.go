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

package source

import (
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/source/codec"
	"github.com/pkg/errors"
)

var (
	ErrSourceNameRequired = errors.New("pipelines[n].source.name is required")
	ErrSourceTypeRequired = errors.New("pipelines[n].source.type is required")
)

type Config struct {
	Enabled         *bool                  `yaml:"enabled,omitempty"`
	Name            string                 `yaml:"name,omitempty"`
	Type            string                 `yaml:"type,omitempty" validate:"required"`
	Properties      cfg.CommonCfg          `yaml:",inline"`
	FieldsUnderRoot bool                   `yaml:"fieldsUnderRoot,omitempty" default:"false"`
	FieldsUnderKey  string                 `yaml:"fieldsUnderKey,omitempty" default:"fields"`
	Fields          map[string]interface{} `yaml:"fields,omitempty"`
	FieldsFromEnv   map[string]string      `yaml:"fieldsFromEnv,omitempty"`
	FieldsFromPath  map[string]string      `yaml:"fieldsFromPath,omitempty"`
	Codec           *codec.Config          `yaml:"codec,omitempty"`
}

func (c *Config) DeepCopy() *Config {
	if c == nil {
		return nil
	}

	var newFields map[string]interface{}
	if c.Fields != nil {
		f := make(map[string]interface{})
		for k, v := range c.Fields {
			f[k] = v
		}
		newFields = f
	}
	var newFieldsFromEnv map[string]string
	if c.FieldsFromEnv != nil {
		fe := make(map[string]string)
		for k, v := range c.FieldsFromEnv {
			fe[k] = v
		}
		newFieldsFromEnv = fe
	}

	var newFieldsFromPath map[string]string
	if c.FieldsFromPath != nil {
		fp := make(map[string]string)
		for k, v := range c.FieldsFromPath {
			fp[k] = v
		}
		newFieldsFromPath = fp
	}

	out := &Config{
		Enabled:         c.Enabled,
		Name:            c.Name,
		Type:            c.Type,
		Properties:      c.Properties.DeepCopy(),
		FieldsUnderRoot: c.FieldsUnderRoot,
		FieldsUnderKey:  c.FieldsUnderKey,
		Fields:          newFields,
		FieldsFromEnv:   newFieldsFromEnv,
		FieldsFromPath:  newFieldsFromPath,
		Codec:           c.Codec.DeepCopy(),
	}

	return out
}

func (c *Config) Validate() error {
	if c.Name == "" {
		return ErrSourceNameRequired
	}
	if c.Type == "" {
		return ErrSourceTypeRequired
	}
	if c.Codec != nil {
		if err := c.Codec.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) Merge(from *Config) {
	if c.Type != from.Type {
		return
	}

	c.Properties = cfg.MergeCommonCfg(c.Properties, from.Properties, false)

	if c.FieldsUnderRoot == false {
		c.FieldsUnderRoot = from.FieldsUnderRoot
	}

	if c.FieldsUnderKey == "" {
		c.FieldsUnderKey = from.FieldsUnderKey
	}

	if c.Fields == nil {
		c.Fields = from.Fields
	} else {
		for k, v := range from.Fields {
			_, ok := c.Fields[k]
			if !ok {
				c.Fields[k] = v
			}
		}
	}

	if c.FieldsFromEnv == nil {
		c.FieldsFromEnv = from.FieldsFromEnv
	} else {
		for k, v := range from.FieldsFromEnv {
			_, ok := c.FieldsFromEnv[k]
			if !ok {
				c.FieldsFromEnv[k] = v
			}
		}
	}

	if c.FieldsFromPath == nil {
		c.FieldsFromPath = from.FieldsFromPath
	} else {
		for k, v := range from.FieldsFromPath {
			_, ok := c.FieldsFromPath[k]
			if !ok {
				c.FieldsFromPath[k] = v
			}
		}
	}

	if c.Codec == nil {
		c.Codec = from.Codec
	} else {
		c.Codec.Merge(from.Codec)
	}
}

func MergeSourceList(base []*Config, from []*Config) []*Config {
	if len(base) == 0 {
		return from
	}
	if len(from) == 0 {
		return base
	}

	fromMap := make(map[string]*Config)
	for _, v := range from {
		fromMap[v.Type] = v
	}

	for _, baseCfg := range base {
		typeName := baseCfg.Type
		fromCfg, ok := fromMap[typeName]
		if ok {
			baseCfg.Merge(fromCfg)
			continue
		}
	}

	return base
}

// InjectRawConfig only the properties in source.Config can be obtained from a single source,
// If this interface is implemented, it can be used to get the source.Config in source
type InjectRawConfig interface {
	SetSourceConfig(config *Config)
}
