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

package elasticsearch

import (
	"time"
)

type Config struct {
	PipelineName  string
	Name          string
	Hosts         []string `yaml:"hosts,omitempty" validate:"required"`
	UserName      string   `yaml:"username,omitempty"`
	Password      string   `yaml:"password,omitempty"`
	Schema        string   `yaml:"schema,omitempty"`
	Sniff         *bool    `yaml:"sniff,omitempty"`
	Gzip          *bool    `yaml:"gzip,omitempty"`
	IncludeFields []string `yaml:"includeFields,omitempty"`
	ExcludeFields []string `yaml:"excludeFields,omitempty"`
	Query         string   `yaml:"query,omitempty"`
	SortBy        []SortBy `yaml:"sortBy,omitempty" validate:"required"`
	Indices       []string `yaml:"indices,omitempty" validate:"required"`
	Size          int      `yaml:"size,omitempty" default:"100"`

	Interval time.Duration `yaml:"interval,omitempty" default:"30s"`
	Timeout  time.Duration `yaml:"timeout,omitempty" default:"5s"`

	DBConfig DBConfig `yaml:"db,omitempty"`
}

type SortBy struct {
	Fields    string `yaml:"fields,omitempty"`
	Ascending bool   `yaml:"ascending,omitempty" default:"true"`
}

func (c *Config) AllSortByFields() []string {
	var all []string
	for _, s := range c.SortBy {
		all = append(all, s.Fields)
	}
	return all
}

func (c *Config) Validate() error {
	return nil
}
