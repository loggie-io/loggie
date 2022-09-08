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

package schema

import "github.com/loggie-io/loggie/pkg/core/interceptor"

type Config struct {
	interceptor.ExtensionConfig `yaml:",inline"`

	// AddMeta add system metadata to the event
	AddMeta *AddMetaConfig `yaml:"addMeta,omitempty"`

	Remap map[string]Remap `yaml:"remap,omitempty"`
}

type AddMetaConfig struct {
	Timestamp    TimestampRemap `yaml:"timestamp,omitempty"`
	PipelineName Remap          `yaml:"pipelineName,omitempty"`
	SourceName   Remap          `yaml:"sourceName,omitempty"`
}

type Remap struct {
	Key string `yaml:"key,omitempty"`
}

type TimestampRemap struct {
	Key      string `yaml:"key,omitempty"`
	Location string `yaml:"location,omitempty"`
	Layout   string `yaml:"layout,omitempty"`
}
