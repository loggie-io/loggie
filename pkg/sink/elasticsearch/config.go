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

package elasticsearch

import "github.com/loggie-io/loggie/pkg/util/pattern"

type Config struct {
	Hosts      []string `yaml:"hosts,omitempty" validate:"required"`
	UserName   string   `yaml:"username,omitempty"`
	Password   string   `yaml:"password,omitempty"`
	Index      string   `yaml:"index,omitempty" validate:"required"`
	Etype      string   `yaml:"etype,omitempty"` // elasticsearch type, for v5.* backward compatibility
	DocumentId string   `yaml:"documentId,omitempty"`
	Schema     string   `yaml:"schema,omitempty"`
	Sniff      *bool    `yaml:"sniff,omitempty"`
	Gzip       *bool    `yaml:"gzip,omitempty"`
}

func (c *Config) Validate() error {
	if err := pattern.Validate(c.Index); err != nil {
		return err
	}

	if err := pattern.Validate(c.DocumentId); err != nil {
		return err
	}

	return nil
}
