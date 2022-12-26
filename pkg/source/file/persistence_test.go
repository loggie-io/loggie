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
	"path/filepath"
	"testing"
	"time"

	"github.com/loggie-io/loggie/pkg/core/log"
)

func Benchmark_dbHandler_write(b *testing.B) {
	log.InitDefaultLogger()
	handler := GetOrCreateShareDbHandler(DbConfig{
		File:         "./data/loggie.db",
		FlushTimeout: 2 * time.Second,
		BufferSize:   1024,
		TableName:    "registry",
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats := make([]*State, 0)
		for i := 0; i < 5; i++ {
			index := i + 1
			stats = append(stats, &State{
				Offset:      123,
				Filename:    fmt.Sprintf("/tmp/loggie/presssure/pressure-access-%d.log", index),
				CollectTime: time.Now(),
				JobUid:      fmt.Sprintf("test-job-uid-%d", index),
				EventUid:    fmt.Sprintf("aaa-%d-%s", index, time2text(time.Now())),
			})
		}
		stats = append(stats, &State{
			Offset:      888,
			Filename:    fmt.Sprintf("/tmp/loggie/presssure/pressure-access-%d.log", 6),
			CollectTime: time.Now(),
			JobUid:      fmt.Sprintf("test-job-uid-%d", 6),
			EventUid:    fmt.Sprintf("aaa-%d-%s", 6, time2text(time.Now())),
		})
		handler.write(stats)
	}
}

func Test_text2time(t *testing.T) {
	log.InitDefaultLogger()
	tt := text2time("2020-11-16 19:57:59.888")
	fmt.Println(time2text(tt))
}

func Test_time2text(t *testing.T) {
	fmt.Println(time2text(time.Now()))
}

func Test_Alter(t *testing.T) {
	dbConfig := DbConfig{
		File:         "./data/loggie.db",
		FlushTimeout: 2 * time.Second,
		BufferSize:   1024,
		TableName:    "registry",
	}
	file := dbConfig.File
	dbFile, err := filepath.Abs(file)
	fmt.Println("db file:" + dbFile)
	db, err := sql.Open(driver, dbFile)
	if err != nil {
		fmt.Println("open err: " + err.Error())
		return
	}
	//result, err := db.Exec(".schema")
	//if err != nil {
	//	fmt.Println("exec err:" + err.Error())
	//	return
	//}
	//fmt.Printf("exec result: %+v \n", result)
	rows, err := db.Query("PRAGMA table_info(registry)")
	if err != nil {
		fmt.Println("query err: " + err.Error())
		return
	}
	type TableDesc struct {
		cid          int
		fieldName    string
		fieldType    string
		notNull      bool
		defaultValue interface{}
		pk           bool
	}
	for rows.Next() {
		var tableDesc TableDesc
		err := rows.Scan(&tableDesc.cid, &tableDesc.fieldName, &tableDesc.fieldType, &tableDesc.notNull, &tableDesc.defaultValue, &tableDesc.pk)
		if err != nil {
			fmt.Println("scan err: " + err.Error())
			return
		}
		fmt.Printf("table desc: %+v \n", tableDesc)
	}
}

func Test_GraceAddColumn(t *testing.T) {
	file := "./data/loggie.db"
	dbFile, err := filepath.Abs(file)
	fmt.Println("db file:" + dbFile)
	db, err := sql.Open(driver, dbFile)
	if err != nil {
		fmt.Println("open err: " + err.Error())
		return
	}

	columnDesc := ColumnDesc{
		fieldName:    "line_number",
		fieldType:    "INTEGER",
		notNull:      false,
		defaultValue: 0,
	}
	alterSql := columnDesc.toAlterSql()
	fmt.Println("alter sql: " + alterSql)
	_, err = db.Exec(alterSql)
	if err != nil {
		fmt.Printf("add column fail: %s \n", err)
	}
}
