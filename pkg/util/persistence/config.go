package persistence

import (
	"sync"
	"time"
)

var (
	globalDbHandler *DbHandler
	dbLock          sync.Mutex
)

type DbConfig struct {
	File                 string        `yaml:"file,omitempty" default:"./data/loggie.db"`
	FlushTimeout         time.Duration `yaml:"flushTimeout,omitempty" default:"2s"`
	BufferSize           int           `yaml:"bufferSize,omitempty" default:"2048"`
	TableName            string        `yaml:"tableName,omitempty" default:"registry"`
	CleanInactiveTimeout time.Duration `yaml:"cleanInactiveTimeout,omitempty" default:"504h"` // default records not updated in 21 days will be deleted
	CleanScanInterval    time.Duration `yaml:"cleanScanInterval,omitempty" default:"1h"`
}

func GetOrCreateShareDbHandler(config DbConfig) *DbHandler {
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
