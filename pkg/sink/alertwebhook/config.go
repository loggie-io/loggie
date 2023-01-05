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

package alertwebhook

import "time"

type Config struct {
	Addr      string            `yaml:"addr,omitempty"`
	Template  string            `yaml:"template,omitempty"`
	Timeout   time.Duration     `yaml:"timeout,omitempty" default:"30s"`
	Headers   map[string]string `yaml:"headers,omitempty"`
	Method    string            `yaml:"method,omitempty"`
	LineLimit int               `yaml:"lineLimit" default:"10"`
}
