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

package logalert

import (
	"regexp"
)

type Config struct {
	Matcher Matcher `yaml:"matcher,omitempty"`
	Labels  Labels  `yaml:"labels,omitempty"`
}

type Matcher struct {
	Regexp       []string `yaml:"regexp,omitempty"`
	Contains     []string `yaml:"contains,omitempty"`
	TargetHeader string   `yaml:"target,omitempty"`
}

type Labels struct {
	FromHeader []string `yaml:"from,omitempty"`
}

func (c *Config) Validate() error {
	if len(c.Matcher.Regexp) != 0 {
		for _, reg := range c.Matcher.Regexp {
			_, err := regexp.Compile(reg)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
