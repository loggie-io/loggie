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

package loki

import "time"

type Config struct {
	URL                string            `yaml:"url,omitempty" validate:"required"`
	ContentType        string            `yaml:"contentType,omitempty" default:"json" validate:"oneof=json protobuf"`
	TenantId           string            `yaml:"tenantId,omitempty"`
	Timeout            time.Duration     `yaml:"timeout,omitempty" default:"30s"`
	EntryLine          string            `yaml:"entryLine,omitempty"`
	Headers            map[string]string `yaml:"header,omitempty"`
	InsecureSkipVerify bool              `yaml:"insecureSkipVerify" default:"false"`
}
