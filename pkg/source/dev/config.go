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

package dev

import "time"

// ContentConfig is a map of key-value pairs for static content.
// The key "body" is treated as the event body, other keys are set as header fields.
type ContentConfig map[string]interface{}

type Config struct {
	Qps         int             `yaml:"qps,omitempty" default:"1000"`
	ByteSize    int             `yaml:"byteSize,omitempty" default:"1024"`
	EventsTotal int64           `yaml:"eventsTotal,omitempty" default:"-1"`
	Interval    time.Duration   `yaml:"interval,omitempty"`
	Contents    []ContentConfig `yaml:"content,omitempty"`
}
