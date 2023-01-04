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

package aggregator

import "time"

type Config struct {
	Interval time.Duration `yaml:"interval,omitempty" defaults:"60s"`
	Select []AggregateSelectConfig `yaml:"select,omitempty" validate:"required"`
	GroupBy []string `yaml:"groupBy,omitempty"`
	GroupByMaxCount int `yaml:"groupByMaxCount,omitempty"`
}

type AggregateSelectConfig struct {
	Key string `yaml:"key,omitempty"`
	Operator string `yaml:"operator,omitempty"`
	As string `yaml:"as,omitempty"`
}

