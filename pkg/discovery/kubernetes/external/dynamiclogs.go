/*
Copyright 2022 Loggie Authors

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

package external

import (
	"fmt"
	"k8s.io/apimachinery/pkg/util/sets"
	"sync"
)

const SystemContainerLogsPath = "containerLogs"

var defaultDynamicLogIndexer *DynamicLogIndexer

// DynamicLogIndexer store all paths corresponding to pipeline/source and associated fields.
type DynamicLogIndexer struct {
	PipelineSourceToPaths map[string][]PathFieldsPair // key: (pipelineName/sourceName), value: file source paths
	PipelineToSource      map[string]sets.String      // key: pipelineName, value: sourceName

	mutex sync.RWMutex
}

type PathFieldsPair struct {
	Paths  []string
	Fields map[string]interface{}
}

func InitDynamicLogIndexer() {
	defaultDynamicLogIndexer = newDynamicLogIndexer()
}

func newDynamicLogIndexer() *DynamicLogIndexer {
	return &DynamicLogIndexer{
		PipelineSourceToPaths: make(map[string][]PathFieldsPair),
		PipelineToSource:      make(map[string]sets.String),
	}
}

func GetDynamicPaths(pipelineName string, sourceName string) ([]PathFieldsPair, bool) {
	if defaultDynamicLogIndexer == nil {
		return []PathFieldsPair{}, false
	}
	defaultDynamicLogIndexer.mutex.RLock()
	defer defaultDynamicLogIndexer.mutex.RUnlock()

	ret, ok := defaultDynamicLogIndexer.PipelineSourceToPaths[uid(pipelineName, sourceName)]
	return ret, ok
}

func SetDynamicPaths(pipelineName string, sourceName string, pair []PathFieldsPair) {
	if defaultDynamicLogIndexer == nil {
		return
	}
	defaultDynamicLogIndexer.mutex.Lock()
	defer defaultDynamicLogIndexer.mutex.Unlock()

	srcSets, ok := defaultDynamicLogIndexer.PipelineToSource[pipelineName]
	if !ok {
		defaultDynamicLogIndexer.PipelineToSource[pipelineName] = sets.NewString(sourceName)
	} else {
		srcSets.Insert(sourceName)
	}

	defaultDynamicLogIndexer.PipelineSourceToPaths[uid(pipelineName, sourceName)] = pair
}

func DelDynamicPaths(pipelineName string) {
	if defaultDynamicLogIndexer == nil {
		return
	}
	defaultDynamicLogIndexer.mutex.Lock()
	defer defaultDynamicLogIndexer.mutex.Unlock()

	sourceSets, ok := defaultDynamicLogIndexer.PipelineToSource[pipelineName]
	if !ok {
		return
	}

	for _, src := range sourceSets.List() {
		delete(defaultDynamicLogIndexer.PipelineSourceToPaths, uid(pipelineName, src))
	}
}

func uid(pipelineName string, sourceName string) string {
	return fmt.Sprintf("%s/%s", pipelineName, sourceName)
}
