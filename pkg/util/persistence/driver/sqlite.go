//go:build !driver_badger

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
	"database/sql"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/persistence/reg"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	driver      = "sqlite3"
	createTable = `
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
	descTableSQL = "PRAGMA table_info(registry)"

	queryAll                          = `SELECT id,pipeline_name,source_name,filename,job_uid,file_offset,collect_time,sys_version,line_number FROM registry`
	insertSql                         = `INSERT INTO registry (pipeline_name,source_name,filename,job_uid,file_offset,collect_time,sys_version,line_number) VALUES (?, ?, ?, ?, ?, ?,?,?)`
	updateSql                         = `UPDATE registry SET file_offset = ?,collect_time = ?,line_number = ? WHERE id = ?`
	queryByJobUidAndSourceAndPipeline = `SELECT id,pipeline_name,source_name,filename,job_uid,file_offset,collect_time,sys_version,line_number FROM registry WHERE job_uid = '%s' AND source_name = "%s" AND pipeline_name = "%s"`
	deleteById                        = `DELETE FROM registry where id = ?`
	updateNameByJobWatchId            = `UPDATE registry SET filename = ? WHERE job_uid = ? AND source_name = ? AND pipeline_name = ?`
	deleteByJobWatchId                = `DELETE FROM registry where job_uid = ? AND source_name = ? AND pipeline_name = ?`
)

type Engine struct {
	db     *sql.DB
	dbFile string

	mutex sync.Mutex
}

func Init(file string) *Engine {
	log.Info("using database engine: sqlite3")
	dbFile := createDbFile(file)
	db, err := sql.Open(driver, dbFile)
	if err != nil {
		panic(fmt.Sprintf("open db(%s) fail: %s", dbFile, err))
	}

	e := &Engine{
		db:     db,
		dbFile: dbFile,
	}
	e.check()

	return e
}

func (e *Engine) Insert(registries []reg.Registry) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.txWrapper(insertSql, func(stmt *sql.Stmt) {
		for _, r := range registries {
			_, err := stmt.Exec(r.PipelineName, r.SourceName, r.Filename, r.JobUid, r.Offset, r.CollectTime, r.Version, r.LineNumber)
			if err != nil {
				log.Error("%s stmt exec fail: %s", e.String(), err)
			}
		}
	})
	return nil
}

func (e *Engine) Close() error {
	return e.db.Close()
}

func (e *Engine) Update(registries []reg.Registry) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.txWrapper(updateSql, func(stmt *sql.Stmt) {
		for _, r := range registries {
			_, err := stmt.Exec(r.Offset, r.CollectTime, r.LineNumber, r.Id)
			if err != nil {
				log.Error("%s stmt exec fail: %s", e.String(), err)
			}
		}
	})

	return nil
}

func (e *Engine) UpdateFileName(rs []reg.Registry) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.txWrapper(updateNameByJobWatchId, func(stmt *sql.Stmt) {
		for _, r := range rs {
			result, err := stmt.Exec(r.Filename, r.JobUid, r.SourceName, r.PipelineName)
			if err != nil {
				log.Error("%s stmt exec fail: %s", e.String(), err)
				continue
			}
			affected, err := result.RowsAffected()
			if err != nil {
				log.Error("%s get result fail: %s", e.String(), err)
				continue
			}
			log.Info("updateName registry(%+v). affected: %d", r, affected)
		}
	})

	return nil
}

func (e *Engine) Delete(r reg.Registry) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	var resErr error
	e.txWrapper(deleteById, func(stmt *sql.Stmt) {
		_, err := stmt.Exec(r.Id)
		if err != nil {
			resErr = errors.WithMessagef(err, "%s stmt exec fail", e.String())
		}
	})

	return resErr
}

func (e *Engine) DeleteBy(jobUid string, sourceName string, pipelineName string) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	var resErr error
	e.txWrapper(deleteByJobWatchId, func(stmt *sql.Stmt) {
		result, err := stmt.Exec(jobUid, sourceName, pipelineName)
		if err != nil {
			resErr = errors.WithMessagef(err, "%s stmt exec fail", e.String())
		}
		_, err = result.RowsAffected()
		if err != nil {
			resErr = errors.WithMessagef(err, "%s get result fail", e.String())
		}
	})
	return resErr
}

func (e *Engine) FindAll() ([]reg.Registry, error) {
	return e.findBySql(queryAll), nil
}

func (e *Engine) FindBy(jobUid string, sourceName string, pipelineName string) (reg.Registry, error) {
	querySql := fmt.Sprintf(queryByJobUidAndSourceAndPipeline, jobUid, sourceName, pipelineName)
	rs := e.findBySql(querySql)
	if len(rs) == 0 {
		return reg.Registry{}, nil
	}
	return rs[0], nil
}

func createDbFile(file string) (dbFile string) {
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

func (e *Engine) check() {
	_, err := e.db.Exec(createTable)
	if err != nil {
		_ = e.db.Close()
		panic(fmt.Sprintf("check table registry fail: %+v", err))
	}
	e.graceAddColumn(ColumnDesc{
		fieldName:    "line_number",
		fieldType:    "INTEGER",
		notNull:      false,
		defaultValue: 0,
	})
}

func (e *Engine) String() string {
	return fmt.Sprintf("db-handler(file:%s)", e.dbFile)
}

func (e *Engine) txWrapper(sqlString string, f func(stmt *sql.Stmt)) {
	tx, err := e.db.Begin()
	if err != nil {
		log.Error("%s begin tx fail: %s", e.String(), err)
	}
	stmt, err := tx.Prepare(sqlString)
	if err != nil {
		log.Error("%s prepare sql fail: %s", e.String(), err)
	}
	defer stmt.Close()
	f(stmt)
	err = tx.Commit()
	if err != nil {
		log.Error("%s tx commit fail: %s", e.String(), err)
	}
}

func (e *Engine) findBySql(querySql string) []reg.Registry {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	rows, err := e.db.Query(querySql)
	if err != nil {
		panic(fmt.Sprintf("%s query registry by sql(%s) fail: %v", e.String(), querySql, err))
	}
	defer rows.Close()
	registries := make([]reg.Registry, 0)
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
			panic(fmt.Sprintf("query registry fail: %v", err))
		}
		registries = append(registries, reg.Registry{
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
		panic(fmt.Sprintf("query registry fail: %v", err))
	}
	return registries
}

func (e *Engine) descTable() []ColumnDesc {
	rows, err := e.db.Query(descTableSQL)
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

func (e *Engine) graceAddColumn(desc ColumnDesc) {
	tableDesc := e.descTable()
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
		_, err := e.db.Exec(alterSql)
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
