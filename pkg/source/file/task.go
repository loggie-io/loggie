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
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

const (
	START = WatchTaskType("start")
	STOP  = WatchTaskType("stop")
)

type WatchTaskType string

type WatchTaskEvent struct {
	watchTaskType WatchTaskType
	watchTask     *WatchTask
}

type WatchTask struct {
	epoch            *pipeline.Epoch
	pipelineName     string
	sourceName       string
	config           CollectConfig
	eventPool        *event.Pool
	productFunc      api.ProductFunc
	activeChan       chan *Job
	countDown        *sync.WaitGroup
	waiteForStopJobs map[string]*Job
	stopTime         time.Time
	sourceFields     map[string]interface{}
}

func NewWatchTask(epoch *pipeline.Epoch, pipelineName string, sourceName string, config CollectConfig,
	eventPool *event.Pool, productFunc api.ProductFunc, activeChan chan *Job, sourceFields map[string]interface{}) *WatchTask {
	w := &WatchTask{
		epoch:        epoch,
		pipelineName: pipelineName,
		sourceName:   sourceName,
		config:       config,
		eventPool:    eventPool,
		productFunc:  productFunc,
		activeChan:   activeChan,
		countDown:    &sync.WaitGroup{},
		sourceFields: sourceFields,
	}
	// init excludeFilePatterns
	l := len(w.config.ExcludeFiles)
	if l > 0 {
		excludeFilePatterns := make([]*regexp.Regexp, l)
		for i, excludeFile := range w.config.ExcludeFiles {
			pattern, err := regexp.Compile(excludeFile)
			if err != nil {
				log.Error("compile exclude file pattern(%s) fail: %v", excludeFile, err)
				continue
			}
			excludeFilePatterns[i] = pattern
		}
		w.config.excludeFilePatterns = excludeFilePatterns
	}
	// init glob path support recursive
	paths := w.config.Paths
	for i, path := range paths {
		if strings.HasSuffix(path, "**") {
			paths[i] = path + "/*"
		}
		if strings.HasSuffix(path, "**/") {
			paths[i] = path + "*"
		}
	}
	return w
}

func (wt *WatchTask) newJob(filename string, info os.FileInfo) *Job {
	return NewJob(wt, filename, info)
}

func (wt *WatchTask) WatchTaskKey() string {
	var watchTaskKey strings.Builder
	watchTaskKey.WriteString(wt.pipelineName)
	watchTaskKey.WriteString(":")
	watchTaskKey.WriteString(wt.sourceName)
	return watchTaskKey.String()
}

func (wt *WatchTask) isParentOf(job *Job) bool {
	return job.task.pipelineName == wt.pipelineName && job.task.sourceName == wt.sourceName
}

func (wt *WatchTask) String() string {
	var watchTaskString strings.Builder
	watchTaskString.WriteString(wt.epoch.String())
	watchTaskString.WriteString(":")
	watchTaskString.WriteString(wt.sourceName)
	return watchTaskString.String()
}

func (wt *WatchTask) StopJobsInfo() string {
	if len(wt.waiteForStopJobs) <= 0 {
		return ""
	}
	var stopJobsInfo strings.Builder
	for _, job := range wt.waiteForStopJobs {
		stopJobsInfo.WriteString("{")
		stopJobsInfo.WriteString(job.WatchUid())
		stopJobsInfo.WriteString(":")
		stopJobsInfo.WriteString(job.filename)
		stopJobsInfo.WriteString("}")
	}
	return stopJobsInfo.String()
}

func (wt *WatchTask) IsStop() bool {
	return !wt.stopTime.IsZero()
}
