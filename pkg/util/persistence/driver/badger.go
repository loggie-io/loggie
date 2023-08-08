//go:build driver_badger

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

package driver

import (
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/json"
	"github.com/loggie-io/loggie/pkg/util/persistence/reg"
	"github.com/pkg/errors"
	"math"
	"time"
)

type Engine struct {
	db *badger.DB

	stop chan struct{}
}

type Logger struct {
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	log.Error(format, args...)
}

func (l *Logger) Warningf(format string, args ...interface{}) {
	log.Warn(format, args...)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	log.Info(format, args...)
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	log.Debug(format, args...)
}

func Init(file string) reg.DbEngine {
	log.Info("using database engine: badger")
	opts := badger.DefaultOptions(file)
	opts.MemTableSize = 5 << 20
	opts.BaseTableSize = 1 << 20
	opts.BaseLevelSize = 2 << 20
	opts.ValueThreshold = 196608
	opts.ValueLogFileSize = 100 << 20
	opts.ValueLogMaxEntries = 1000

	logger := &Logger{}
	db, err := badger.Open(opts.WithLogger(logger))
	if err != nil {
		log.Fatal("open db(%s) fail: %s", file, err)
	}

	eg := &Engine{
		db:   db,
		stop: make(chan struct{}),
	}

	go eg.PeriodGC()
	return eg
}

func (e *Engine) Insert(registries []reg.Registry) error {
	lists := Group(registries, 100)
	for _, list := range lists {
		_ = e.db.Update(func(txn *badger.Txn) error {
			for _, registry := range list {
				key := registry.Key()
				value := registry.Value()
				err := txn.Set(key, value)
				if err != nil {
					return errors.WithMessagef(err, "insert registry %s fail", key)
				}
				log.Debug("inserted registry %s: %s", key, value)
			}
			return nil
		})
	}
	return nil
}

func (e *Engine) Close() error {
	close(e.stop)
	return e.db.Close()
}

func (e *Engine) PeriodGC() {
	ticker := time.NewTicker(10 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-e.stop:
			return

		case <-ticker.C:
			log.Info("start running badger value log gc")
			err := e.db.RunValueLogGC(0.7)
			if err != nil {
				log.Info("try to run gc: %s", err)
			}
		}
	}
}

func (e *Engine) Update(registries []reg.Registry) error {
	lists := Group(registries, 100)
	for _, list := range lists {
		_ = e.db.Update(func(txn *badger.Txn) error {
			for _, registry := range list {
				key := registry.Key()
				var value []byte
				if !registry.CheckIntegrity() {
					item, err := txn.Get(key)
					if err != nil {
						log.Error("fail to get registry %s: %s", key, err)
						continue
					}
					oldRegistry := reg.Registry{}
					valueCopy, err := item.ValueCopy(nil)
					if err != nil {
						log.Error("fail to get registry %s bytes: %s", key, err)
						continue
					}
					err = json.Unmarshal(valueCopy, &oldRegistry)
					if err != nil {
						log.Error("fail to decode registry %s: %s", key, err)
						continue
					}
					log.Debug("get old version registry before updating %s", valueCopy)
					oldRegistry.Merge(registry)
					value = oldRegistry.Value()
				} else {
					value = registry.Value()
				}

				err := txn.Set(key, value)
				if err != nil {
					return err
				}
				log.Debug("updated registry %s : %s", key, value)
			}
			return nil
		})
	}

	return nil
}

func (e *Engine) UpdateFileName(rs []reg.Registry) error {
	return e.Update(rs)
}

func (e *Engine) Delete(r reg.Registry) error {
	return e.db.Update(func(txn *badger.Txn) error {
		key := r.Key()
		err := txn.Delete(key)
		if err != nil {
			return err
		}
		return nil
	})
}

func (e *Engine) DeleteBy(jobUid string, sourceName string, pipelineName string) error {
	return e.db.Update(func(txn *badger.Txn) error {
		key := reg.GenKey(jobUid, sourceName, pipelineName)
		err := txn.Delete(key)
		if err != nil {
			return err
		}
		return nil
	})
}

func (e *Engine) FindAll() ([]reg.Registry, error) {
	list := make([]reg.Registry, 0)
	_ = e.db.View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()

		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			item := iterator.Item()
			_ = item.Value(func(val []byte) error {
				registry := reg.Registry{}
				err := json.Unmarshal(val, &registry)
				if err != nil {
					return errors.WithMessagef(err, "fail to decode registry %s", item.Key())
				}
				list = append(list, registry)
				return nil
			})
		}
		return nil
	})

	return list, nil
}

func (e *Engine) FindBy(jobUid string, sourceName string, pipelineName string) (reg.Registry, error) {
	registry := reg.Registry{}
	key := reg.GenKey(jobUid, sourceName, pipelineName)
	_ = e.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		valueCopy, err := item.ValueCopy(nil)
		if err != nil {
			log.Error("fail to get registry %s bytes: %s", key, err)
			return err
		}
		err = json.Unmarshal(valueCopy, &registry)
		if err != nil {
			log.Error("fail to decode registry %s: %s", key, err)
			registry = reg.Registry{}
			return err
		}
		return nil
	})

	return registry, nil
}

func Group(rs []reg.Registry, size int) []reg.RegistryList {
	l := len(rs)
	if l <= size {
		return []reg.RegistryList{rs}
	}

	listSize := math.Ceil(float64(l) / float64(size))
	result := make([]reg.RegistryList, int(listSize))
	for i := 0; i < len(result); i++ {
		start := i * size
		end := start + size
		if end > l {
			end = l
		}
		result[i] = rs[start:end]
	}
	return result
}
