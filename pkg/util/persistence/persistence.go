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
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	_ "github.com/mattn/go-sqlite3"
)

const (
	driver            = "sqlite3"
	TimeFormatPattern = "2006-01-02 15:04:05.999"
	createTable       = `
	CREATE TABLE IF NOT EXISTS registry (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		pipeline_name TEXT NOT NULL,
		source_name TEXT NOT NULL,
		filename TEXT NOT NULL,
		job_uid TEXT NOT NULL,
		file_offset INTEGER NOT NULL,
		collect_time TEXT NULL,
		sys_version TEXT NOT NULL,
		line_number INTEGER default 0
	);`
	descTable = "PRAGMA table_info(registry)"

	queryAll                          = `SELECT id,pipeline_name,source_name,filename,job_uid,file_offset,collect_time,sys_version,line_number FROM registry`
	insertSql                         = `INSERT INTO registry (pipeline_name,source_name,filename,job_uid,file_offset,collect_time,sys_version,line_number) VALUES (?, ?, ?, ?, ?, ?,?,?)`
	updateSql                         = `UPDATE registry SET file_offset = ?,collect_time = ?,line_number = ? WHERE id = ?`
	queryByJobUidAndSourceAndPipeline = `SELECT id,pipeline_name,source_name,filename,job_uid,file_offset,collect_time,sys_version,line_number FROM registry WHERE job_uid = '%s' AND source_name = "%s" AND pipeline_name = "%s"`
	deleteById                        = `DELETE FROM registry where id = ?`
	updateNameByJobWatchId            = `UPDATE registry SET filename = ? WHERE job_uid = ? AND source_name = ? AND pipeline_name = ?`
	deleteByJobWatchId                = `DELETE FROM registry where job_uid = ? AND source_name = ? AND pipeline_name = ?`

	DeleteByIdOpt               = DbOptType(1)
	DeleteByJobUidOpt           = DbOptType(2)
	UpsertOffsetByJobWatchIdOpt = DbOptType(3)
	UpdateNameByJobWatchIdOpt   = DbOptType(4)
)

type Registry struct {
	Id           int    `json:"id"`
	PipelineName string `json:"pipelineName"`
	SourceName   string `json:"sourceName"`
	Filename     string `json:"filename"`
	JobUid       string `json:"jobUid"`
	Offset       int64  `json:"offset"`
	CollectTime  string `json:"collectTime"`
	Version      string `json:"version"`
	LineNumber   int64  `json:"lineNumber"`
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
	db        *sql.DB
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

func (d *DbHandler) createDbFile() (dbFile string) {
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

func (d *DbHandler) check() {
	_, err := d.db.Exec(createTable)
	if err != nil {
		_ = d.db.Close()
		panic(fmt.Sprintf("%s check table registry fail: %s", d.String(), err))
	}
	d.graceAddColumn(ColumnDesc{
		fieldName:    "line_number",
		fieldType:    "INTEGER",
		notNull:      false,
		defaultValue: 0,
	})
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
	d.txWrapper(deleteById, func(stmt *sql.Stmt) {
		_, err := stmt.Exec(r.Id)
		if err != nil {
			log.Error("%s stmt exec fail: %s", d.String(), err)
		}
		log.Info("delete registry(id: %d) because db.cleanInactiveTimeout(%dh) reached. file: %s", r.Id, d.config.CleanInactiveTimeout/time.Hour, r.Filename)
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
	d.txWrapper(insertSql, func(stmt *sql.Stmt) {
		for _, r := range registries {
			_, err := stmt.Exec(r.PipelineName, r.SourceName, r.Filename, r.JobUid, r.Offset, r.CollectTime, r.Version, r.LineNumber)
			if err != nil {
				log.Error("%s stmt exec fail: %s", d.String(), err)
			}
		}
	})
}

func (d *DbHandler) updateRegistry(registries []Registry) {
	d.txWrapper(updateSql, func(stmt *sql.Stmt) {
		for _, r := range registries {
			_, err := stmt.Exec(r.Offset, r.CollectTime, r.LineNumber, r.Id)
			if err != nil {
				log.Error("%s stmt exec fail: %s", d.String(), err)
			}
		}
	})
}

func (d *DbHandler) updateName(rs []Registry) {
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

func (d *DbHandler) deleteRemoved(rs []Registry) {
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

func (d *DbHandler) upsertOffsetByJobWatchId(r Registry) {
	r.CollectTime = time2text(time.Now())
	r.Version = api.VERSION
	rs := []Registry{r}

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

func (d *DbHandler) txWrapper(sqlString string, f func(stmt *sql.Stmt)) {
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

func (d *DbHandler) FindAll() []Registry {
	// start := time.Now()
	// defer func() {
	//	cost := time.Since(start).Milliseconds()
	//	log.Info("find by source cost: %dms", cost)
	// }()
	return d.findBySql(queryAll)
}

func (d *DbHandler) FindBy(jobUid string, sourceName string, pipelineName string) Registry {
	querySql := fmt.Sprintf(queryByJobUidAndSourceAndPipeline, jobUid, sourceName, pipelineName)
	rs := d.findBySql(querySql)
	if len(rs) == 0 {
		return Registry{}
	}
	return rs[0]
}

func (d *DbHandler) findBySql(querySql string) []Registry {
	rows, err := d.db.Query(querySql)
	if err != nil {
		panic(fmt.Sprintf("%s query registry by sql(%s) fail: %v", d.String(), querySql, err))
	}
	defer rows.Close()
	registries := make([]Registry, 0)
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
			line_number   int64
		)
		err = rows.Scan(&id, &pipeline_name, &source_name, &filename, &job_uid, &file_offset, &collect_time, &sys_version, &line_number)
		if err != nil {
			panic(fmt.Sprintf("%s query registry fail: %v", d.String(), err))
		}
		registries = append(registries, Registry{
			Id:           id,
			PipelineName: pipeline_name,
			SourceName:   source_name,
			Filename:     filename,
			JobUid:       job_uid,
			Offset:       file_offset,
			CollectTime:  collect_time,
			Version:      sys_version,
			LineNumber:   line_number,
		})
	}
	err = rows.Err()
	if err != nil {
		panic(fmt.Sprintf("db(%s) query registry fail: %v", d.config.File, err))
	}
	return registries
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
			d.updateName([]Registry{r})
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

func contain(registries []Registry, state *State) (id int, ok bool) {
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

func (d *dbHandler) descTable() []ColumnDesc {
	rows, err := d.db.Query(descTable)
	if err != nil {
		log.Error("desc table fail : %s", err)
		return nil
	}
	tableDesc := make([]ColumnDesc, 0)
	for rows.Next() {
		var columnDesc ColumnDesc
		err := rows.Scan(&columnDesc.cid, &columnDesc.fieldName, &columnDesc.fieldType,
			&columnDesc.notNull, &columnDesc.defaultValue, &columnDesc.pk)
		if err != nil {
			fmt.Println("desc table scan err: " + err.Error())
			return nil
		}
		tableDesc = append(tableDesc, columnDesc)
	}
	return tableDesc
}

func (d *dbHandler) graceAddColumn(desc ColumnDesc) {
	tableDesc := d.descTable()
	if tableDesc == nil {
		return
	}
	exist := false
	for _, columnDesc := range tableDesc {
		if columnDesc.fieldName == desc.fieldName {
			exist = true
		}
	}
	if !exist {
		alterSql := desc.toAlterSql()
		log.Info("alter sql: %s", alterSql)
		_, err := d.db.Exec(alterSql)
		if err != nil {
			log.Error("add column fail: %s", err)
		}
	}
}

type ColumnDesc struct {
	cid          int
	fieldName    string
	fieldType    string
	notNull      bool
	defaultValue interface{}
	pk           bool
}

func (cd ColumnDesc) toAlterSql() string {
	var alterSql strings.Builder
	alterSql.Grow(128)
	alterSql.WriteString("alter table registry add column ")
	alterSql.WriteString(cd.fieldName)
	alterSql.WriteString(" ")
	alterSql.WriteString(cd.fieldType)
	alterSql.WriteString(" ")
	if cd.defaultValue != nil {
		alterSql.WriteString(" default ")
		alterSql.WriteString(fmt.Sprintf("%v", cd.defaultValue))
		alterSql.WriteString(" ")
	} else {
		if cd.notNull {
			alterSql.WriteString(" NOT NULL")
		} else {

			alterSql.WriteString(" NULL")
		}
	}
	return alterSql.String()
}
