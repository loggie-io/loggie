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

package elasticsearch

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
	CREATE TABLE IF NOT EXISTS source_record (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		pipeline_name TEXT NOT NULL,
		source_name TEXT NOT NULL,
		source TEXT NOT NULL,
		offset TEXT NOT NULL,
		collect_time TEXT NULL,
		sys_version TEXT NOT NULL
	);`

	insertSql                = `INSERT INTO source_record (pipeline_name, source_name, source, offset, collect_time,sys_version) VALUES (?, ?, ?, ?, ?, ?)`
	updateSql                = `UPDATE source_record SET offset = ?, collect_time = ? WHERE id = ?`
	queryBySourceAndPipeline = `SELECT * FROM source_record WHERE source_name = "%s" AND pipeline_name = "%s"`
	deleteById               = `DELETE FROM source_record where id = ?`
)

var (
	globalDbHandler *dbHandler
	dbLock          sync.Mutex
)

type registry struct {
	Id           int    `json:"id"`
	PipelineName string `json:"pipelineName"`
	SourceName   string `json:"sourceName"`
	Source       string `json:"source"`
	Offset       string `json:"offset"`
	CollectTime  string `json:"collectTime"`
	Version      string `json:"version"`
}

type DbConfig struct {
	File                 string        `yaml:"file,omitempty" default:"./data/loggie.db"`
	FlushTimeout         time.Duration `yaml:"flushTimeout,omitempty" default:"2s"`
	CleanInactiveTimeout time.Duration `yaml:"cleanInactiveTimeout,omitempty" default:"504h"` // default records not updated in 21 days will be deleted
	CleanScanInterval    time.Duration `yaml:"cleanScanInterval,omitempty" default:"1h"`
}

type dbHandler struct {
	done   chan struct{}
	config DbConfig
	db     *sql.DB
	dbFile string
}

func GetOrCreateShareDbHandler(config DbConfig) *dbHandler {
	if globalDbHandler != nil {
		return globalDbHandler
	}
	dbLock.Lock()
	defer dbLock.Unlock()
	if globalDbHandler != nil {
		return globalDbHandler
	}
	globalDbHandler = newDbHandler(config)
	return globalDbHandler
}

func newDbHandler(config DbConfig) *dbHandler {
	d := &dbHandler{
		done:   make(chan struct{}),
		config: config,
	}
	dbFile := d.createDbFile()
	d.dbFile = dbFile
	db, err := sql.Open(driver, dbFile)
	if err != nil {
		panic(fmt.Sprintf("open db(%s) fail: %s", dbFile, err))
	}
	d.db = db
	d.check()
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
		panic(fmt.Sprintf("%s check table source_record fail: %s", d.String(), err))
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
}

func (d *dbHandler) delete(r registry) {
	d.txWrapper(deleteById, func(stmt *sql.Stmt) {
		_, err := stmt.Exec(r.Id)
		if err != nil {
			log.Error("%s stmt exec fail: %s", d.String(), err)
		}
		log.Info("delete registry(id: %d) because db.cleanInactiveTimeout(%dh) reached. source name: %s, source: %s", r.Id, d.config.CleanInactiveTimeout/time.Hour, r.SourceName, r.Source)
	})
}

func (d *dbHandler) insertRegistry(registries []registry) {
	d.txWrapper(insertSql, func(stmt *sql.Stmt) {
		for _, r := range registries {
			_, err := stmt.Exec(r.PipelineName, r.SourceName, r.Source, r.Offset, r.CollectTime, r.Version)
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

func (d *dbHandler) upsertRecord(r registry) {
	r.CollectTime = time2text(time.Now())
	r.Version = api.VERSION
	rs := []registry{r}

	or := d.findBy(r.SourceName, r.PipelineName)
	if or.SourceName != "" {
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

func (d *dbHandler) findBy(sourceName string, pipelineName string) registry {
	querySql := fmt.Sprintf(queryBySourceAndPipeline, sourceName, pipelineName)
	rs := d.findBySql(querySql)
	if len(rs) == 0 {
		return registry{}
	}
	return rs[0]
}

func (d *dbHandler) findBySql(querySql string) []registry {
	rows, err := d.db.Query(querySql)
	if err != nil {
		panic(fmt.Sprintf("%s query source_record fail: %v", d.String(), err))
	}
	defer rows.Close()
	registries := make([]registry, 0)
	for rows.Next() {
		var (
			id           int
			pipelineName string
			sourceName   string
			source       string
			offset       string
			collectTime  string
			sysVersion   string
		)
		err = rows.Scan(&id, &pipelineName, &sourceName, &source, &offset, &collectTime, &sysVersion)
		if err != nil {
			panic(fmt.Sprintf("%s query source_record fail: %v", d.String(), err))
		}
		registries = append(registries, registry{
			Id:           id,
			PipelineName: pipelineName,
			SourceName:   sourceName,
			Source:       source,
			Offset:       offset,
			CollectTime:  collectTime,
			Version:      sysVersion,
		})
	}
	err = rows.Err()
	if err != nil {
		panic(fmt.Sprintf("db(%s) query source_record fail: %v", d.config.File, err))
	}
	return registries
}

func time2text(date time.Time) string {
	return date.Format(timeFormatPattern)
}
