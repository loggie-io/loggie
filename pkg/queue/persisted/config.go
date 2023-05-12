/*
Copyright 2023 Loggie Authors

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

package persisted

import (
	"github.com/loggie-io/loggie/pkg/util/size"
	"time"
)

const serializeJson = "json"

type Config struct {
	BatchSize          int           `yaml:"batchSize" default:"512"`
	BatchBytes         size.Size     `yaml:"batchBytes"`
	BatchAggMaxTimeout time.Duration `yaml:"batchAggTimeout" default:"1s"`

	// The directory where the data is stored
	Path string `yaml:"path,omitempty" default:"./data/queue"`
	// The maximum size of a single queue
	MaxFileBytes size.Size `yaml:"maxFileBytes,omitempty"`
	// The number of individual files that the queue split stores
	MaxFileCount int64 `yaml:"maxFileCount,omitempty" default:"10"`

	// The maximum number of batches to be synced to disk
	SyncCount int64 `yaml:"syncCount,omitempty" default:"512"`
	// Maximum timeout for syncing to disk files
	SyncTimeout time.Duration `yaml:"syncTimeout,omitempty" default:"1s"`

	SerializationType string `yaml:"serializationType,omitempty"`
}

func (c *Config) SetDefaults() {
	if c.BatchBytes.Bytes == 0 {
		// default: 8MB
		// batchMaxBytes must less than maxFileSize
		c.BatchBytes.Bytes = 8 * 1024 * 1024
	}

	if c.MaxFileBytes.Bytes == 0 {
		// default: 100MB
		c.MaxFileBytes.Bytes = 100 * 1024 * 1024
	}

	if c.SerializationType == "" {
		c.SerializationType = serializeJson
	}
}
