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

package persistence

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
)

const (
	TimeFormatPattern = "2006-01-02 15:04:05.999"

	DeleteByIdOpt               = DbOptType(1)
	DeleteByJobUidOpt           = DbOptType(2)
	UpsertOffsetByJobWatchIdOpt = DbOptType(3)
	UpdateNameByJobWatchIdOpt   = DbOptType(4)
)

type Registry struct {
	PipelineName string `json:"pipelineName"`
	SourceName   string `json:"sourceName"`
	Filename     string `json:"filename"`
	JobUid       string `json:"jobUid"`
	Offset       int64  `json:"offset"`
	CollectTime  string `json:"collectTime"`
	Version      string `json:"version"`
	LineNumber   int64  `json:"lineNumber"`
}

func (r *Registry) key() []byte {
	return genKey(r.JobUid, r.SourceName, r.PipelineName)
}

func genKey(jobuid, source, pipeline string) []byte {
	var bb bytes.Buffer
	bb.WriteString(jobuid)
	bb.WriteString("/")
	bb.WriteString(source)
	bb.WriteString("/")
	bb.WriteString(pipeline)
	return bb.Bytes()
}

func (r *Registry) value() []byte {
	marshal, _ := json.Marshal(r)
	return marshal
}

func (r *Registry) checkIntegrity() bool {
	return len(r.Filename) > 0 &&
		r.Offset > 0 &&
		len(r.CollectTime) > 0 &&
		len(r.Version) > 0 &&
		r.LineNumber > 0
}

func (r *Registry) merge(registry Registry) {
	if len(registry.Filename) > 0 {
		r.Filename = registry.Filename
	}

	if registry.Offset > 0 {
		r.Offset = registry.Offset
	}

	if len(registry.CollectTime) > 0 {
		r.CollectTime = registry.CollectTime
	}

	if len(registry.Version) > 0 {
		r.Version = registry.Version
	}

	if registry.LineNumber > 0 {
		r.LineNumber = registry.LineNumber
	}
}

type registryList []Registry

func group(rs []Registry, size int) []registryList {
	l := len(rs)
	if l <= size {
		return []registryList{rs}
	}

	listSize := math.Ceil(float64(l) / float64(size))
	result := make([]registryList, int(listSize))
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

type compressStatPair struct {
	first *State
	last  *State
}

type DbOptType int

type DbOpt struct {
	R           Registry
	OptType     DbOptType
	Immediately bool
}

type DbHandler struct {
	done      chan struct{}
	config    DbConfig
	State     chan *State
	db        *badger.DB
	dbFile    string
	countDown sync.WaitGroup
	optChan   chan DbOpt
}

func NewDbHandler(config DbConfig) *DbHandler {
	d := &DbHandler{
		done:    make(chan struct{}),
		config:  config,
		State:   make(chan *State, config.BufferSize),
		optChan: make(chan DbOpt),
	}

	d.dbFile = d.config.File
	db, err := badger.Open(badger.DefaultOptions(d.dbFile).WithLogger(nil))
	if err != nil {
		panic(fmt.Sprintf("open db(%s) fail: %s", d.dbFile, err))
	}

	d.db = db

	go d.run()
	return d
}

func (d *DbHandler) String() string {
	return fmt.Sprintf("db-handler(file:%s)", d.dbFile)
}

func (d *DbHandler) Stop() {
	close(d.done)
	if d.db != nil {
		err := d.db.Close()
		if err != nil {
			log.Error("close db fail: %s", err)
		}
	}
	d.countDown.Wait()
}

func (d *DbHandler) HandleOpt(opt DbOpt) {
	d.optChan <- opt
}

func (d *DbHandler) run() {
	log.Info("registry db start")
	d.countDown.Add(1)
	var (
		flushD     = time.NewTicker(d.config.FlushTimeout)
		cleanScanD = time.NewTicker(d.config.CleanScanInterval)
		bufferSize = d.config.BufferSize
		buffer     = make([]*State, 0, bufferSize)
		flush      = func() {
			d.write(buffer)
			buffer = make([]*State, 0, bufferSize)
		}
		optBufferSize = 16
		optBuffer     = make([]DbOpt, 0, optBufferSize)
		optFlush      = func() {
			d.processOpt(optBuffer)
			optBuffer = make([]DbOpt, 0, optBufferSize)
		}
	)
	defer func() {
		d.countDown.Done()
		flushD.Stop()
		cleanScanD.Stop()
		log.Info("registry db stop")
	}()
	for {
		select {
		case <-d.done:
			return
		case s := <-d.State:
			buffer = append(buffer, s)
			if len(buffer) >= bufferSize {
				flush()
			}
		case o := <-d.optChan:
			if o.Immediately {
				d.processOpt([]DbOpt{o})
			} else {
				optBuffer = append(optBuffer, o)
				if len(optBuffer) >= optBufferSize {
					optFlush()
				}
			}
		case <-flushD.C:
			if len(buffer) > 0 {
				flush()
			}
			if len(optBuffer) > 0 {
				optFlush()
			}
		case <-cleanScanD.C:
			d.cleanData()
		}
	}
}

func (d *DbHandler) cleanData() {
	registries := d.FindAll()
	for _, r := range registries {
		collectTime := r.CollectTime
		if collectTime == "" {
			continue
		}
		t := text2time(collectTime)
		if time.Since(t) >= d.config.CleanInactiveTimeout {
			// delete
			d.delete(r)
		}
	}
}

func (d *DbHandler) delete(r Registry) {

	_ = d.db.Update(func(txn *badger.Txn) error {
		key := r.key()
		err := txn.Delete(key)
		if err != nil {
			log.Error("%s fail to delete registry %s : %s", d.String(), key, err)
			return err
		}
		log.Info("delete registry %s because db.cleanInactiveTimeout(%dh) reached. file: %s", key, d.config.CleanInactiveTimeout/time.Hour, r.Filename)
		return nil
	})
}

// only one thread invoke,without lock
func (d *DbHandler) write(stats []*State) {
	// start := time.Now()
	// defer func() {
	//	cost := time.Since(start).Milliseconds()
	//	log.Info("write cost: %dms", cost)
	// }()

	css := compressStats(stats)

	registries := d.FindAll()
	insertRegistries := make([]Registry, 0)
	updateRegistries := make([]Registry, 0)

	for _, cs := range css {
		stat := cs.last
		if ok := contain(registries, stat); ok {
			r := d.state2Registry(stat)
			updateRegistries = append(updateRegistries, r)
		} else {
			log.Warn("The registry record corresponding to stat(%+v) has been deleted, stat will be ignore!", stat)
		}
	}

	if len(insertRegistries) > 0 {
		d.insertRegistry(insertRegistries)
	}

	if len(updateRegistries) > 0 {
		d.updateRegistry(updateRegistries)
	}
}

func (d *DbHandler) state2Registry(stat *State) Registry {
	return Registry{
		PipelineName: stat.PipelineName,
		SourceName:   stat.SourceName,
		Filename:     stat.Filename,
		JobUid:       stat.JobUid,
		Offset:       stat.NextOffset,
		CollectTime:  time2text(stat.CollectTime),
		Version:      api.VERSION,
		LineNumber:   stat.LineNumber,
	}
}

func (d *DbHandler) insertRegistry(registries []Registry) {
	lists := group(registries, 100)
	for _, list := range lists {
		_ = d.db.Update(func(txn *badger.Txn) error {
			for _, registry := range list {
				key := registry.key()
				value := registry.value()
				err := txn.Set(key, value)
				if err != nil {
					log.Error("%s insert registry %s fail: %s", d.String(), key, err)
				}
				log.Debug("inserted registry %s: %s", key, value)
			}
			return nil
		})
	}

}

func (d *DbHandler) updateRegistry(registries []Registry) {
	lists := group(registries, 100)
	for _, list := range lists {
		_ = d.db.Update(func(txn *badger.Txn) error {
			for _, registry := range list {
				key := registry.key()
				var value []byte
				if !registry.checkIntegrity() {
					item, err := txn.Get(key)
					if err != nil {
						log.Error("fail to get registry %s: %s", key, err)
						continue
					}
					oldRegistry := Registry{}
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
					oldRegistry.merge(registry)
					value = oldRegistry.value()
				} else {
					value = registry.value()
				}

				err := txn.Set(key, value)
				if err != nil {
					log.Error("%s update registry %s fail: %s", d.String(), key, err)
				}
				log.Debug("updated registry %s : %s", key, value)
			}
			return nil
		})
	}
}

func (d *DbHandler) deleteRemoved(rs []Registry) {
	_ = d.db.Update(func(txn *badger.Txn) error {
		for _, registry := range rs {
			key := registry.key()
			err := txn.Delete(key)
			if err != nil {
				log.Error("%s delete registry %s fail: %s", d.String(), key, err)
			}
			log.Info("delete registry(%+v). ", registry)
		}
		return nil
	})
}

func (d *DbHandler) upsertOffsetByJobWatchId(r Registry) {
	r.CollectTime = time2text(time.Now())
	r.Version = api.VERSION
	rs := []Registry{r}

	or := d.FindBy(r.JobUid, r.SourceName, r.PipelineName)
	if or.JobUid != "" {
		// update
		d.updateRegistry(rs)
	} else {
		// insert
		d.insertRegistry(rs)
	}
}

func (d *DbHandler) FindAll() []Registry {
	// start := time.Now()
	// defer func() {
	//	cost := time.Since(start).Milliseconds()
	//	log.Info("find by source cost: %dms", cost)
	// }()
	list := make([]Registry, 0)
	_ = d.db.View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()

		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			item := iterator.Item()
			_ = item.Value(func(val []byte) error {
				registry := Registry{}
				err := json.Unmarshal(val, &registry)
				if err != nil {
					log.Error("fail to decode registry %s: %s", item.Key(), err)
					return err
				}
				list = append(list, registry)
				return nil
			})
		}
		return nil
	})

	return list
}

func (d *DbHandler) FindBy(jobUid string, sourceName string, pipelineName string) Registry {
	registry := Registry{}
	key := genKey(jobUid, sourceName, pipelineName)
	_ = d.db.View(func(txn *badger.Txn) error {
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
			registry = Registry{}
			return err
		}
		return nil
	})

	return registry
}

func (d *DbHandler) processOpt(optBuffer []DbOpt) {
	for _, opt := range optBuffer {
		optType := opt.OptType
		r := opt.R
		if optType == DeleteByIdOpt {
			d.delete(r)
			continue
		}
		if optType == DeleteByJobUidOpt {
			d.deleteRemoved([]Registry{r})
			continue
		}
		if optType == UpsertOffsetByJobWatchIdOpt {
			d.upsertOffsetByJobWatchId(r)
			continue
		}
		if optType == UpdateNameByJobWatchIdOpt {
			d.updateRegistry([]Registry{r})
			continue
		}
	}
}

func text2time(date string) time.Time {
	location, err := time.ParseInLocation(TimeFormatPattern, date, time.Local)
	if err != nil {
		log.Error("convert text to time fail: %v", err)
	}
	return location
}

func time2text(date time.Time) string {
	return date.Format(TimeFormatPattern)
}

func contain(registries []Registry, state *State) (ok bool) {
	for _, r := range registries {
		if r.PipelineName == state.PipelineName && r.SourceName == state.SourceName && r.JobUid == state.JobUid {
			return true
		}
	}
	return false
}

func compressStats(stats []*State) []compressStatPair {
	if len(stats) == 1 {
		css := make([]compressStatPair, 1)
		css[0] = compressStatPair{
			first: stats[0],
			last:  stats[0],
		}
		return css
	}
	jobKey2Cs := make(map[string]compressStatPair)
	for _, stat := range stats {
		jobKey := stat.WatchUid
		if cs, ok := jobKey2Cs[jobKey]; ok {
			if cs.first.Offset > stat.Offset {
				cs.first = stat
			}
			if cs.last.Offset < stat.Offset {
				cs.last = stat
			}
			jobKey2Cs[jobKey] = cs
		} else {
			jobKey2Cs[jobKey] = compressStatPair{
				first: stat,
				last:  stat,
			}
		}
	}

	compressStatPairs := make([]compressStatPair, 0, len(jobKey2Cs))
	for _, cs := range jobKey2Cs {
		compressStatPairs = append(compressStatPairs, cs)
	}
	return compressStatPairs
}
