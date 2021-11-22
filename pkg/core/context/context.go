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

package context

import (
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/cfg"
)

func NewContext(name string, typename api.Type, category api.Category, properties cfg.CommonCfg) api.Context {
	return &DefaultContext{
		name:       name,
		category:   category,
		typename:   typename,
		properties: properties,
	}
}

type DefaultContext struct {
	name       string
	category   api.Category
	typename   api.Type
	properties cfg.CommonCfg
}

func (c *DefaultContext) Properties() cfg.CommonCfg {
	return c.properties
}

func (c *DefaultContext) Category() api.Category {
	return c.category
}

func (c *DefaultContext) Type() api.Type {
	return c.typename
}

func (c *DefaultContext) Name() string {
	return c.name
}
