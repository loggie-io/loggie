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

package persistence

import (
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/core/log"
)

var (
	globalDbHandler *DbHandler
	dbLock          sync.Mutex
	config          DbConfig
	_DRIVER_        string
)

type DbConfig struct {
	File                 string        `yaml:"file,omitempty"`
	FlushTimeout         time.Duration `yaml:"flushTimeout,omitempty" default:"2s"`
	BufferSize           int           `yaml:"bufferSize,omitempty" default:"2048"`
	TableName            string        `yaml:"tableName,omitempty" default:"registry"`
	CleanInactiveTimeout time.Duration `yaml:"cleanInactiveTimeout,omitempty" default:"504h"` // default records not updated in 21 days will be deleted
	CleanScanInterval    time.Duration `yaml:"cleanScanInterval,omitempty" default:"1h"`
}

func (d *DbConfig) SetDefaults() {
	if d.File == "" {
		if _DRIVER_ == DriverBadger {
			d.File = "./data"
		} else {
			d.File = "./data/loggie.db"
		}
	}
}

func SetConfig(dbConfig DbConfig) {
	log.Debug("db config set %+v", dbConfig)
	config = dbConfig
}

func GetConfig() DbConfig {
	return config
}

func GetOrCreateShareDbHandler() *DbHandler {
	if globalDbHandler != nil {
		return globalDbHandler
	}
	dbLock.Lock()
	defer dbLock.Unlock()
	if globalDbHandler != nil {
		return globalDbHandler
	}
	globalDbHandler = NewDbHandler(config)
	return globalDbHandler
}

func StopDbHandler() {
	if globalDbHandler == nil {
		return
	}
	dbLock.Lock()
	defer dbLock.Unlock()
	globalDbHandler.Stop()
}
