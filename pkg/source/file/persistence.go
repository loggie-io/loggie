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
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	_ "github.com/mattn/go-sqlite3"
)

const (
	driver            = "sqlite3"
	timeFormatPattern = "2006-01-02 15:04:05.999"
	createTable       = `
	CREATE TABLE IF NOT EXISTS registry (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		pipeline_name TEXT NOT NULL,
		source_name TEXT NOT NULL,
		filename TEXT NOT NULL,
		job_uid TEXT NOT NULL,
		file_offset INTEGER NOT NULL,
		collect_time TEXT NULL,
		sys_version TEXT NOT NULL
	);`

	queryAll                          = `SELECT id,pipeline_name,source_name,filename,job_uid,file_offset,collect_time,sys_version FROM registry`
	insertSql                         = `INSERT INTO registry (pipeline_name,source_name,filename,job_uid,file_offset,collect_time,sys_version) VALUES (?, ?, ?, ?, ?, ?,?)`
	updateSql                         = `UPDATE registry SET file_offset = ?,collect_time = ? WHERE id = ?`
	queryByJobUidAndSourceAndPipeline = `SELECT id,pipeline_name,source_name,filename,job_uid,file_offset,collect_time,sys_version FROM registry WHERE job_uid = '%s' AND source_name = "%s" AND pipeline_name = "%s"`
	deleteById                        = `DELETE FROM registry where id = ?`
	updateNameByJobWatchId            = `UPDATE registry SET filename = ? WHERE job_uid = ? AND source_name = ? AND pipeline_name = ?`
	deleteByJobWatchId                = `DELETE FROM registry where job_uid = ? AND source_name = ? AND pipeline_name = ?`

	DeleteByIdOpt               = DbOptType(1)
	DeleteByJobUidOpt           = DbOptType(2)
	UpsertOffsetByJobWatchIdOpt = DbOptType(3)
	UpdateNameByJobWatchIdOpt   = DbOptType(4)
)

type registry struct {
	Id           int    `json:"id"`
	PipelineName string `json:"pipelineName"`
	SourceName   string `json:"sourceName"`
	Filename     string `json:"filename"`
	JobUid       string `json:"jobUid"`
	Offset       int64  `json:"offset"`
	CollectTime  string `json:"collectTime"`
	Version      string `json:"version"`
}

type compressStatPair struct {
	first *State
	last  *State
}

type DbOptType int

type DbOpt struct {
	r           registry
	optType     DbOptType
	immediately bool
}

type dbHandler struct {
	done      chan struct{}
	config    DbConfig
	state     chan *State
	db        *sql.DB
	dbFile    string
	countDown sync.WaitGroup
	optChan   chan DbOpt
}

func newDbHandler(config DbConfig) *dbHandler {
	d := &dbHandler{
		done:    make(chan struct{}),
		config:  config,
		state:   make(chan *State, config.BufferSize),
		optChan: make(chan DbOpt),
	}
	dbFile := d.createDbFile()
	d.dbFile = dbFile
	db, err := sql.Open(driver, dbFile)
	if err != nil {
		panic(fmt.Sprintf("open db(%s) fail: %s", dbFile, err))
	}
	d.db = db
	d.check()

	go d.run()
	return d
}

func (d *dbHandler) createDbFile() (dbFile string) {
	file := d.config.File
	dbFile, err := filepath.Abs(file)
	if err != nil {
		panic(fmt.Sprintf("get db abs file(%s) fail： %s", file, err))
	}
	log.Info("db file: %s", dbFile)
	_, err = os.Stat(dbFile)
	if err == nil || os.IsExist(err) {
		return dbFile
	}
	dir := filepath.Dir(file)
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Warn("create dir(%s) fail: %s", dir, err)
	}
	_, err = os.Create(dbFile)
	if err != nil {
		log.Warn("create db file(%s) fail: %s", dbFile, err)
	}
	return dbFile
}

func (d *dbHandler) check() {
	_, err := d.db.Exec(createTable)
	if err != nil {
		_ = d.db.Close()
		panic(fmt.Sprintf("%s check table registry fail: %s", d.String(), err))
	}
}

func (d *dbHandler) String() string {
	return fmt.Sprintf("db-handler(file:%s)", d.dbFile)
}

func (d *dbHandler) Stop() {
	close(d.done)
	if d.db != nil {
		err := d.db.Close()
		if err != nil {
			log.Error("close db fail: %s", err)
		}
	}
	d.countDown.Wait()
}

func (d *dbHandler) HandleOpt(opt DbOpt) {
	d.optChan <- opt
}

func (d *dbHandler) run() {
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
		case s := <-d.state:
			buffer = append(buffer, s)
			if len(buffer) >= bufferSize {
				flush()
			}
		case o := <-d.optChan:
			if o.immediately {
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

func (d *dbHandler) cleanData() {
	registries := d.findAll()
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

func (d *dbHandler) delete(r registry) {
	d.txWrapper(deleteById, func(stmt *sql.Stmt) {
		_, err := stmt.Exec(r.Id)
		if err != nil {
			log.Error("%s stmt exec fail: %s", d.String(), err)
		}
		log.Info("delete registry(id: %d) because db.cleanInactiveTimeout(%dh) reached. file: %s", r.Id, d.config.CleanInactiveTimeout/time.Hour, r.Filename)
	})
}

// only one thread invoke,without lock
func (d *dbHandler) write(stats []*State) {
	// start := time.Now()
	// defer func() {
	//	cost := time.Since(start).Milliseconds()
	//	log.Info("write cost: %dms", cost)
	// }()

	css := compressStats(stats)

	registries := d.findAll()
	insertRegistries := make([]registry, 0)
	updateRegistries := make([]registry, 0)

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

func (d *dbHandler) state2Registry(stat *State) registry {
	return registry{
		PipelineName: stat.PipelineName,
		SourceName:   stat.SourceName,
		Filename:     stat.Filename,
		JobUid:       stat.JobUid,
		Offset:       stat.NextOffset,
		CollectTime:  time2text(stat.CollectTime),
		Version:      api.VERSION,
	}
}

func (d *dbHandler) insertRegistry(registries []registry) {
	d.txWrapper(insertSql, func(stmt *sql.Stmt) {
		for _, r := range registries {
			_, err := stmt.Exec(r.PipelineName, r.SourceName, r.Filename, r.JobUid, r.Offset, r.CollectTime, r.Version)
			if err != nil {
				log.Error("%s stmt exec fail: %s", d.String(), err)
			}
		}
	})
}

func (d *dbHandler) updateRegistry(registries []registry) {
	d.txWrapper(updateSql, func(stmt *sql.Stmt) {
		for _, r := range registries {
			_, err := stmt.Exec(r.Offset, r.CollectTime, r.Id)
			if err != nil {
				log.Error("%s stmt exec fail: %s", d.String(), err)
			}
		}
	})
}

func (d *dbHandler) updateName(rs []registry) {
	d.txWrapper(updateNameByJobWatchId, func(stmt *sql.Stmt) {
		for _, r := range rs {
			result, err := stmt.Exec(r.Filename, r.JobUid, r.SourceName, r.PipelineName)
			if err != nil {
				log.Error("%s stmt exec fail: %s", d.String(), err)
				continue
			}
			affected, err := result.RowsAffected()
			if err != nil {
				log.Error("%s get result fail: %s", d.String(), err)
				continue
			}
			log.Info("updateName registry(%+v). affected: %d", r, affected)
		}
	})
}

func (d *dbHandler) deleteRemoved(rs []registry) {
	d.txWrapper(deleteByJobWatchId, func(stmt *sql.Stmt) {
		for _, r := range rs {
			result, err := stmt.Exec(r.JobUid, r.SourceName, r.PipelineName)
			if err != nil {
				log.Error("%s stmt exec fail: %s", d.String(), err)
				continue
			}
			affected, err := result.RowsAffected()
			if err != nil {
				log.Error("%s get result fail: %s", d.String(), err)
				continue
			}
			log.Info("delete registry(%+v). affected: %d", r, affected)
		}
	})
}

func (d *dbHandler) upsertOffsetByJobWatchId(r registry) {
	r.CollectTime = time2text(time.Now())
	r.Version = api.VERSION
	rs := []registry{r}

	or := d.findBy(r.JobUid, r.SourceName, r.PipelineName)
	if or.JobUid != "" {
		// update
		r.Id = or.Id
		d.updateRegistry(rs)
	} else {
		// insert
		d.insertRegistry(rs)
	}
}

func (d *dbHandler) txWrapper(sqlString string, f func(stmt *sql.Stmt)) {
	tx, err := d.db.Begin()
	if err != nil {
		log.Error("%s begin tx fail: %s", d.String(), err)
	}
	stmt, err := tx.Prepare(sqlString)
	if err != nil {
		log.Error("%s prepare sql fail: %s", d.String(), err)
	}
	defer stmt.Close()
	f(stmt)
	err = tx.Commit()
	if err != nil {
		log.Error("%s tx commit fail: %s", d.String(), err)
	}
}

func (d *dbHandler) findAll() []registry {
	// start := time.Now()
	// defer func() {
	//	cost := time.Since(start).Milliseconds()
	//	log.Info("find by source cost: %dms", cost)
	// }()
	return d.findBySql(queryAll)
}

func (d *dbHandler) findBy(jobUid string, sourceName string, pipelineName string) registry {
	querySql := fmt.Sprintf(queryByJobUidAndSourceAndPipeline, jobUid, sourceName, pipelineName)
	rs := d.findBySql(querySql)
	if len(rs) == 0 {
		return registry{}
	}
	return rs[0]
}

func (d *dbHandler) findBySql(querySql string) []registry {
	rows, err := d.db.Query(querySql)
	if err != nil {
		panic(fmt.Sprintf("%s query registry fail: %v", d.String(), err))
	}
	defer rows.Close()
	registries := make([]registry, 0)
	for rows.Next() {
		var (
			id            int
			pipeline_name string
			source_name   string
			filename      string
			job_uid       string
			file_offset   int64
			collect_time  string
			sys_version   string
		)
		err = rows.Scan(&id, &pipeline_name, &source_name, &filename, &job_uid, &file_offset, &collect_time, &sys_version)
		if err != nil {
			panic(fmt.Sprintf("%s query registry fail: %v", d.String(), err))
		}
		registries = append(registries, registry{
			Id:           id,
			PipelineName: pipeline_name,
			SourceName:   source_name,
			Filename:     filename,
			JobUid:       job_uid,
			Offset:       file_offset,
			CollectTime:  collect_time,
			Version:      sys_version,
		})
	}
	err = rows.Err()
	if err != nil {
		panic(fmt.Sprintf("db(%s) query registry fail: %v", d.config.File, err))
	}
	return registries
}

func (d *dbHandler) processOpt(optBuffer []DbOpt) {
	for _, opt := range optBuffer {
		optType := opt.optType
		r := opt.r
		if optType == DeleteByIdOpt {
			d.delete(r)
			continue
		}
		if optType == DeleteByJobUidOpt {
			d.deleteRemoved([]registry{r})
			continue
		}
		if optType == UpsertOffsetByJobWatchIdOpt {
			d.upsertOffsetByJobWatchId(r)
			continue
		}
		if optType == UpdateNameByJobWatchIdOpt {
			d.updateName([]registry{r})
			continue
		}
	}
}

func text2time(date string) time.Time {
	location, err := time.ParseInLocation(timeFormatPattern, date, time.Local)
	if err != nil {
		log.Error("convert text to time fail: %v", err)
	}
	return location
}

func time2text(date time.Time) string {
	return date.Format(timeFormatPattern)
}

func contain(registries []registry, state *State) (id int, ok bool) {
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
		jobKey := stat.WatchUid()
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
