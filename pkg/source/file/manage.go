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
	"sync"
)

var (
	globalMultilineProcessor *MultiProcessor
	multilineLock            sync.Mutex

	globalAckChainHandler *AckChainHandler
	ackLock               sync.Mutex

	globalDbHandler *dbHandler
	dbLock          sync.Mutex

	globalWatcher *Watcher
	watchLock     sync.Mutex

	globalReader *Reader
	readLock     sync.Mutex

	pipelineReaderMap  = make(map[string]*Reader)
	pipelineReaderLock sync.Mutex

	sourceReaderMap  = make(map[string]*Reader)
	sourceReaderLock sync.Mutex
)

const (
	IsolationPipeline = IsolationLevel("pipeline")
	IsolationSource   = IsolationLevel("source")
	IsolationShare    = IsolationLevel("share")
)

type IsolationLevel string

type Isolation struct {
	Level        IsolationLevel
	PipelineName string
	SourceName   string
}

func StopReader(isolation Isolation) {
	if isolation.Level == IsolationShare {
		return
	}
	if isolation.Level == IsolationPipeline {
		pipelineReaderLock.Lock()
		defer pipelineReaderLock.Unlock()
		if r, ok := pipelineReaderMap[isolation.PipelineName]; ok {
			r.Stop()
			delete(pipelineReaderMap, isolation.PipelineName)
		}
		return
	}
	if isolation.Level == IsolationSource {
		key := isolation.PipelineName + ":" + isolation.SourceName
		sourceReaderLock.Lock()
		defer sourceReaderLock.Unlock()
		if r, ok := sourceReaderMap[key]; ok {
			r.Stop()
			delete(sourceReaderMap, key)
		}
	}
}

func GetOrCreateReader(isolation Isolation, readerConfig ReaderConfig, watcher *Watcher) *Reader {
	if isolation.Level == IsolationPipeline {
		pipelineName := isolation.PipelineName
		r, ok := pipelineReaderMap[pipelineName]
		if ok {
			return r
		}
		pipelineReaderLock.Lock()
		defer pipelineReaderLock.Unlock()
		r, ok = pipelineReaderMap[pipelineName]
		if ok {
			return r
		}
		reader := newReader(readerConfig, watcher)
		pipelineReaderMap[pipelineName] = reader
		return reader
	}
	if isolation.Level == IsolationSource {
		pipelineName := isolation.PipelineName
		sourceName := isolation.SourceName
		key := pipelineName + ":" + sourceName
		r, ok := sourceReaderMap[key]
		if ok {
			return r
		}
		sourceReaderLock.Lock()
		defer sourceReaderLock.Unlock()
		r, ok = sourceReaderMap[key]
		if ok {
			return r
		}
		reader := newReader(readerConfig, watcher)
		sourceReaderMap[key] = reader
		return reader
	}
	return GetOrCreateShareReader(readerConfig, watcher)
}

func GetOrCreateShareReader(readerConfig ReaderConfig, watcher *Watcher) *Reader {
	if globalReader != nil {
		return globalReader
	}
	readLock.Lock()
	defer readLock.Unlock()
	if globalReader != nil {
		return globalReader
	}
	globalReader = newReader(readerConfig, watcher)
	return globalReader
}

func GetOrCreateShareWatcher(watchConfig WatchConfig, dbConfig DbConfig) *Watcher {
	if globalWatcher != nil {
		return globalWatcher
	}
	watchLock.Lock()
	defer watchLock.Unlock()
	if globalWatcher != nil {
		return globalWatcher
	}
	globalWatcher = newWatcher(watchConfig, GetOrCreateShareDbHandler(dbConfig))
	return globalWatcher
}

func GetOrCreateShareAckChainHandler(sinkCount int, ackConfig AckConfig) *AckChainHandler {
	if globalAckChainHandler != nil {
		return globalAckChainHandler
	}
	ackLock.Lock()
	defer ackLock.Unlock()
	if globalAckChainHandler != nil {
		return globalAckChainHandler
	}
	globalAckChainHandler = NewAckChainHandler(sinkCount, ackConfig)
	return globalAckChainHandler
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

func GetOrCreateShareMultilineProcessor() *MultiProcessor {
	if globalMultilineProcessor != nil {
		return globalMultilineProcessor
	}
	multilineLock.Lock()
	defer multilineLock.Unlock()
	if globalMultilineProcessor != nil {
		return globalMultilineProcessor
	}
	globalMultilineProcessor = NewMultiProcessor()
	return globalMultilineProcessor
}
