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
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"testing"
	"time"

	"github.com/loggie-io/loggie/pkg/core/log"
)

func Benchmark_dbHandler_write(b *testing.B) {
	log.InitDefaultLogger()
	SetConfig(DbConfig{
		File:                 b.TempDir(),
		FlushTimeout:         2 * time.Second,
		BufferSize:           1024,
		TableName:            "registry",
		CleanInactiveTimeout: time.Hour,
		CleanScanInterval:    time.Hour,
	})
	handler := GetOrCreateShareDbHandler()
	defer StopDbHandler()
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

func Test_group(t *testing.T) {
	log.InitDefaultLogger()

	tests := []int{706, 400, 90}

	for _, test := range tests {
		t.Run(fmt.Sprintf("test %d", test), func(t *testing.T) {
			size := test
			data := make([]Registry, size)
			for i := 0; i < len(data); i++ {
				data[i].PipelineName = "a"
			}

			lists := group(data, 100)
			count := 0
			log.Info("total size %d list size %d", size, len(lists))
			for _, list := range lists {
				count += len(list)
			}

			if count != size {
				t.Errorf("total count should be same")
			}
		})
	}

}

func Test_dbActions(t *testing.T) {
	log.InitDefaultLogger()
	SetConfig(DbConfig{
		File:                 t.TempDir(),
		FlushTimeout:         2 * time.Second,
		BufferSize:           1024,
		TableName:            "registry",
		CleanInactiveTimeout: time.Hour,
		CleanScanInterval:    time.Hour,
	})
	handler := GetOrCreateShareDbHandler()
	defer StopDbHandler()

	pipelineName := "pipe"
	sourceName := "test-source"
	jobUid := "test-job-uid"

	timeNow := time.Now()
	handler.insertRegistry([]Registry{{
		PipelineName: pipelineName,
		SourceName:   sourceName,
		Filename:     "temp.log",
		JobUid:       jobUid,
		Offset:       0,
		CollectTime:  time2text(timeNow.Add(time.Minute * (-10))),
		Version:      api.VERSION,
		LineNumber:   1,
	}})

	if handler.FindBy(jobUid, sourceName, pipelineName).JobUid == "" || len(handler.FindAll()) != 1 {
		t.Errorf("new registry should be created")
	}

	handler.updateRegistry([]Registry{{
		PipelineName: pipelineName,
		SourceName:   sourceName,
		JobUid:       jobUid,
		Offset:       1024,
	}})

	updatedRegistry := handler.FindBy(jobUid, sourceName, pipelineName)
	log.Info("updated registry %s", updatedRegistry.value())
	if !updatedRegistry.checkIntegrity() || updatedRegistry.Offset != 1024 {
		t.Errorf("fail to update registry")
	}

	handler.updateRegistry([]Registry{{
		PipelineName: pipelineName,
		SourceName:   sourceName,
		JobUid:       jobUid,
		CollectTime:  time2text(timeNow),
	}})

	updatedRegistry = handler.FindBy(jobUid, sourceName, pipelineName)
	log.Info("updated registry %s", updatedRegistry.value())
	if !updatedRegistry.checkIntegrity() || updatedRegistry.CollectTime != time2text(timeNow) {
		t.Errorf("fail to update registry")
	}

	handler.updateRegistry([]Registry{{
		PipelineName: pipelineName,
		SourceName:   sourceName,
		Filename:     "temp1.log",
		JobUid:       jobUid,
	}})

	updatedRegistry = handler.FindBy(jobUid, sourceName, pipelineName)
	log.Info("updated registry %s", updatedRegistry.value())
	if !updatedRegistry.checkIntegrity() || updatedRegistry.Filename != "temp1.log" {
		t.Errorf("fail to update registry")
	}

	handler.deleteRemoved([]Registry{{
		PipelineName: pipelineName,
		SourceName:   sourceName,
		JobUid:       jobUid,
	}})

	if handler.FindBy(jobUid, sourceName, pipelineName).JobUid != "" || len(handler.FindAll()) != 0 {
		t.Errorf("new registry should be delete")
	}

}
