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

package maxbytes

import "github.com/loggie-io/loggie/pkg/core/interceptor"

const Order = 500

type Config struct {
	interceptor.ExtensionConfig `yaml:",inline"`
	// The maximum number of bytes that a single log message can have.
	// All bytes after max_bytes are discarded and not sent.
	// The default is 128KB (131072)
	MaxBytes int    `yaml:"maxBytes,omitempty" default:"131072" validate:"gte=0"`
	Target   string `yaml:"target,omitempty" default:"body"`
}

func (c *Config) SetDefaults() {
	if c != nil {
		c.ExtensionConfig.Order = Order
	}
}
