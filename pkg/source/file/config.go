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

package file

import (
	"os"
	"regexp"
	"time"

	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util"
)

type Config struct {
	AckConfig     AckConfig              `yaml:"ack,omitempty"`
	DbConfig      DbConfig               `yaml:"db,omitempty"`
	WatchConfig   WatchConfig            `yaml:"watcher,omitempty"`
	ReaderConfig  ReaderConfig           `yaml:",inline,omitempty"`
	CollectConfig CollectConfig          `yaml:",inline,omitempty" validate:"required,dive"`
	Isolation     string                 `yaml:"isolation,omitempty" default:"pipeline"`
	Fields        map[string]interface{} `yaml:"fields,omitempty"`
}

type CollectConfig struct {
	IsolationLevel           string        `yaml:"isolationLevel,omitempty" default:"share"`
	Paths                    []string      `yaml:"paths,omitempty" validate:"required"` // glob pattern
	ExcludeFiles             []string      `yaml:"excludeFiles,omitempty"`              // regular pattern
	IgnoreOlder              util.Duration `yaml:"ignoreOlder,omitempty"`
	IgnoreSymlink            bool          `yaml:"ignoreSymlink,omitempty" default:"false"`
	RereadTruncated          bool          `yaml:"rereadTruncated,omitempty" default:"true"`                           // Read from the beginning when the file is truncated
	FirstNBytesForIdentifier int           `yaml:"firstNBytesForIdentifier,omitempty" default:"128" validate:"gte=10"` // If the file size is smaller than `firstNBytesForIdentifier`, it will not be collected
	AddonMeta                bool          `yaml:"addonMeta,omitempty"`
	excludeFilePatterns      []*regexp.Regexp
}

type LineDelimiterValue struct {
	Charset   string `yaml:"charset,omitempty" default:"utf-8"`
	LineType  string `yaml:"type,omitempty" default:"auto"`
	LineValue string `yaml:"value,omitempty" default:"\n"`
}

func (cc CollectConfig) IsIgnoreOlder(info os.FileInfo) bool {
	ignoreOlder := cc.IgnoreOlder
	return ignoreOlder.Duration() > 0 && time.Since(info.ModTime()) > ignoreOlder.Duration()
}

func (cc CollectConfig) IsFileInclude(file string) bool {
	for _, path := range cc.Paths {
		match, err := util.MatchWithRecursive(path, file)
		if err != nil {
			log.Error("path glob pattern(%s) match error: %v", path, err)
			continue
		}
		if match {
			return true
		}
	}
	return false
}

func (cc CollectConfig) IsFileExcluded(file string) bool {
	if len(cc.excludeFilePatterns) == 0 {
		return false
	}
	for _, pattern := range cc.excludeFilePatterns {
		if pattern.Match([]byte(file)) {
			return true
		}
	}
	return false
}

type DbConfig struct {
	File                 string        `yaml:"file,omitempty" default:"./data/loggie.db"`
	FlushTimeout         time.Duration `yaml:"flushTimeout,omitempty" default:"2s"`
	BufferSize           int           `yaml:"bufferSize,omitempty" default:"2048"`
	TableName            string        `yaml:"tableName,omitempty" default:"registry"`
	CleanInactiveTimeout time.Duration `yaml:"cleanInactiveTimeout,omitempty" default:"504h"` // default records not updated in 21 days will be deleted
	CleanScanInterval    time.Duration `yaml:"cleanScanInterval,omitempty" default:"1h"`
}

type AckConfig struct {
	Enable              bool          `yaml:"enable,omitempty" default:"true"`
	MaintenanceInterval time.Duration `yaml:"maintenanceInterval,omitempty" default:"20h"`
}

type WatchConfig struct {
	EnableOsWatch             bool          `yaml:"enableOsWatch,omitempty" default:"true"`
	ScanTimeInterval          time.Duration `yaml:"scanTimeInterval,omitempty" default:"10s"`
	MaintenanceInterval       time.Duration `yaml:"maintenanceInterval,omitempty" default:"5m"`
	CleanFiles                *CleanFiles   `yaml:"cleanFiles,omitempty"`
	FdHoldTimeoutWhenInactive time.Duration `yaml:"fdHoldTimeoutWhenInactive,omitempty" default:"5m"`
	FdHoldTimeoutWhenRemove   time.Duration `yaml:"fdHoldTimeoutWhenRemove,omitempty" default:"5m"`
	MaxOpenFds                int           `yaml:"maxOpenFds,omitempty" default:"512"`
	MaxEofCount               int           `yaml:"maxEofCount,omitempty" default:"3"`
	CleanWhenRemoved          bool          `yaml:"cleanWhenRemoved,omitempty" default:"true"`
	ReadFromTail              bool          `yaml:"readFromTail,omitempty" default:"false"`
	TaskStopTimeout           time.Duration `yaml:"taskStopTimeout,omitempty" default:"30s"`
	CleanDataTimeout          time.Duration `yaml:"cleanDataTimeout,omitempty" default:"5s"`
}

type CleanFiles struct {
	MaxHistoryDays int `yaml:"maxHistoryDays,omitempty"`
}

type ReaderConfig struct {
	LineDelimiter          LineDelimiterValue `yaml:"lineDelimiter,omitempty"`
	WorkerCount            int                `yaml:"workerCount,omitempty" default:"1"`
	ReadChanSize           int                `yaml:"readChanSize,omitempty" default:"512"`     // deprecated
	ReadBufferSize         int                `yaml:"readBufferSize,omitempty" default:"65536"` // The buffer size used for the file reading. default 65536 = 64k = 16*PAGE_SIZE
	MaxContinueRead        int                `yaml:"maxContinueRead,omitempty" default:"16"`
	MaxContinueReadTimeout time.Duration      `yaml:"maxContinueReadTimeout,omitempty" default:"3s"`
	InactiveTimeout        time.Duration      `yaml:"inactiveTimeout,omitempty" default:"3s"`
	MultiConfig            MultiConfig        `yaml:"multi,omitempty"`
	CleanDataTimeout       time.Duration      `yaml:"cleanDataTimeout,omitempty" default:"5s"`

	readChanSize int // readChanSize equals WatchConfig.MaxOpenFds
}

type MultiConfig struct {
	Active   bool          `yaml:"active,omitempty" default:"false"`
	Pattern  string        `yaml:"pattern,omitempty"`
	MaxLines int           `yaml:"maxLines,omitempty" default:"500"`
	MaxBytes int64         `yaml:"maxBytes,omitempty" default:"131072"` // default 128KB
	Timeout  time.Duration `yaml:"timeout,omitempty" default:"5s"`      // default 2 * read.timeout
}
