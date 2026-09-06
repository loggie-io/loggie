/*
Copyright 2026 Loggie Authors

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

package persistence

import "time"

type Config struct {
	BatchSize          int           `yaml:"batchSize" default:"2048"`
	BatchBytes         int64         `yaml:"batchBytes" default:"33554432"` // default:32MB
	BatchAggMaxTimeout time.Duration `yaml:"batchAggTimeout" default:"1s"`

	// DataDir is the root directory holding the on-disk queue data.
	// Each pipeline/queue pair gets its own sub-directory, so one Loggie
	// instance can run multiple persistence queues concurrently.
	DataDir string `yaml:"dataDir" default:"./data/queue"`
	// MaxBytes limits the on-disk backlog. When the un-acked backlog exceeds it,
	// In() blocks, propagating backpressure to the sources.
	MaxBytes int64 `yaml:"maxBytes" default:"1073741824"` // default:1GB
	// SyncWrites forces fsync on every write batch. Safer but slower.
	SyncWrites bool `yaml:"syncWrites"`
	// WriteBufferSize is the max number of events aggregated into a single
	// write transaction against the on-disk store.
	WriteBufferSize int `yaml:"writeBufferSize" default:"1024"`
	// ValueLogGCInterval controls how often the value log garbage collection runs.
	ValueLogGCInterval time.Duration `yaml:"valueLogGCInterval" default:"10m"`
}
