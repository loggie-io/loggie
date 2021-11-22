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
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/log"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestNewDB(t *testing.T) {
	type args struct {
		sourceName string
		config     DbConfig
	}
	tests := []struct {
		name string
		args args
		want *dbHandler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetOrCreateShareDbHandler(tt.args.config); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewDB() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dbHandler_findAll(t *testing.T) {
	type fields struct {
		done       chan struct{}
		sourceName string
		config     DbConfig
		eventChan  chan api.Event
		db         *sql.DB
	}
	tests := []struct {
		name   string
		fields fields
		want   []registry
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &dbHandler{
				done:   tt.fields.done,
				config: tt.fields.config,
				db:     tt.fields.db,
			}
			if got := d.findAll(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("findAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dbHandler_write(t *testing.T) {
	log.InitLog()
	handler := GetOrCreateShareDbHandler(DbConfig{
		Async:        true,
		File:         "./data/loggie.db",
		FlushTimeout: 2 * time.Second,
		BufferSize:   1024,
		TableName:    "registry",
	})
	rand.Seed(time.Now().UnixNano())
	stats := make([]*State, 0)
	for i := 0; i < 5; i++ {
		index := i + 1
		stats = append(stats, &State{
			Offset:      rand.Int63(),
			Filename:    fmt.Sprintf("/tmp/loggie/pressure/pressure-access-%d.log", index),
			CollectTime: time.Now(),
			JobUid:      fmt.Sprintf("test-job-uid-%d", index),
			EventUid:    fmt.Sprintf("aaa-%d-%s", index, time2text(time.Now())),
		})
	}
	stats = append(stats, &State{
		Offset:      0,
		Filename:    fmt.Sprintf("/tmp/loggie/pressure/pressure-access-%d.log", 6),
		CollectTime: time.Now(),
		JobUid:      fmt.Sprintf("test-job-uid-%d", 6),
		EventUid:    fmt.Sprintf("aaa-%d-%s", 6, time2text(time.Now())),
	})
	handler.write(stats)
}

func Benchmark_dbHandler_write(b *testing.B) {
	log.InitLog()
	handler := GetOrCreateShareDbHandler(DbConfig{
		Async:        true,
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
	log.InitLog()
	tt := text2time("2020-11-16 19:57:59.888")
	fmt.Println(time2text(tt))
}

func Test_time2text(t *testing.T) {
	fmt.Println(time2text(time.Now()))
}

func Test_compressStats(t *testing.T) {

}
