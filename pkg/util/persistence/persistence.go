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

package persistence

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/util/persistence/driver"
	"github.com/loggie-io/loggie/pkg/util/persistence/reg"
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
)

const (
	DriverSqlite = "sqlite"
	DriverBadger = "badger"

	TimeFormatPattern = "2006-01-02 15:04:05.999"

	DeleteByIdOpt               = DbOptType(1)
	DeleteByJobUidOpt           = DbOptType(2)
	UpsertOffsetByJobWatchIdOpt = DbOptType(3)
	UpdateNameByJobWatchIdOpt   = DbOptType(4)
)

type compressStatPair struct {
	first *State
	last  *State
}

type DbOptType int

type DbOpt struct {
	R           reg.Registry
	OptType     DbOptType
	Immediately bool
}

type DbHandler struct {
	done      chan struct{}
	config    DbConfig
	State     chan *State
	db        reg.DbEngine
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
	d.db = driver.Init(d.dbFile)

	go d.run()
	return d
}

func (d *DbHandler) HandleOpt(opt DbOpt) {
	d.optChan <- opt
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
			d.deleteRemoved([]reg.Registry{r})
			continue
		}
		if optType == UpsertOffsetByJobWatchIdOpt {
			d.upsertOffsetByJobWatchId(r)
			continue
		}
		if optType == UpdateNameByJobWatchIdOpt {
			d.updateFileName([]reg.Registry{r})
			continue
		}
	}
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

func (d *DbHandler) String() string {
	return fmt.Sprintf("db-handler(file:%s)", d.dbFile)
}

func (d *DbHandler) FindAll() []reg.Registry {
	result, err := d.db.FindAll()
	if err != nil {
		log.Error("find all registry error: %+v", err)
		return nil
	}

	return result
}

func (d *DbHandler) FindBy(jobUid string, sourceName string, pipelineName string) reg.Registry {
	result, err := d.db.FindBy(jobUid, sourceName, pipelineName)
	if err != nil {
		log.Error("find registry by jobUid: %s, sourceName: %s, pipelineName: %s failed: %+v", jobUid, sourceName, pipelineName, err)
		return reg.Registry{}
	}

	return result
}

// only one thread invoke,without lock
func (d *DbHandler) write(stats []*State) {
	css := compressStats(stats)

	registries := d.FindAll()
	insertRegistries := make([]reg.Registry, 0)
	updateRegistries := make([]reg.Registry, 0)

	for _, cs := range css {
		stat := cs.last
		r := d.state2Registry(stat)
		if id, ok := contain(registries, stat); ok {
			tempId := id
			r.Id = tempId
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

func (d *DbHandler) state2Registry(stat *State) reg.Registry {
	return reg.Registry{
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

func (d *DbHandler) insertRegistry(registries []reg.Registry) {
	if err := d.db.Insert(registries); err != nil {
		log.Error("insert registry fail: %s", err)
	}
}

func (d *DbHandler) updateRegistry(registries []reg.Registry) {
	if err := d.db.Update(registries); err != nil {
		log.Error("update registry fail: %+v", err)
	}
}

func (d *DbHandler) updateFileName(registries []reg.Registry) {
	if err := d.db.UpdateFileName(registries); err != nil {
		log.Error("update registry file name fail: %+v", err)
	}
}

func (d *DbHandler) upsertOffsetByJobWatchId(r reg.Registry) {
	r.CollectTime = time2text(time.Now())
	r.Version = api.VERSION
	rs := []reg.Registry{r}

	or := d.FindBy(r.JobUid, r.SourceName, r.PipelineName)
	if or.JobUid != "" {
		// update
		r.Id = or.Id
		d.updateRegistry(rs)
	} else {
		// insert
		d.insertRegistry(rs)
	}
}

func (d *DbHandler) delete(r reg.Registry) {
	if err := d.db.Delete(r); err != nil {
		log.Error("%s fail to delete registry %s : %s", d.String(), r.Key(), err)
		return
	}
	log.Info("delete registry %s because db.cleanInactiveTimeout(%dh) reached. file: %s", r.Key(), d.config.CleanInactiveTimeout/time.Hour, r.Filename)
}

func (d *DbHandler) deleteRemoved(rs []reg.Registry) {
	for _, registry := range rs {
		err := d.db.DeleteBy(registry.JobUid, registry.SourceName, registry.PipelineName)
		if err != nil {
			log.Error("%s delete registry fail: %s", d.String(), err)
			return
		}
		log.Info("delete registry(%+v). ", registry)
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

func contain(registries []reg.Registry, state *State) (id int, ok bool) {
	for _, r := range registries {
		if r.PipelineName == state.PipelineName && r.SourceName == state.SourceName && r.JobUid == state.JobUid {
			return r.Id, true
		}
	}
	return -1, false
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
