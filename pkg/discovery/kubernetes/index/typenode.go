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

package index

import (
	"github.com/loggie-io/loggie/pkg/control"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"sync"
)

type LogConfigTypeNodeIndex struct {
	pipeConfigs map[string]*TypeNodePipeConfig // key: logConfigNamespace/Name, value: pipeline configs

	mutex sync.RWMutex
}

type TypeNodePipeConfig struct {
	Raw []pipeline.Config
	Lgc *v1beta1.LogConfig
}

func NewLogConfigTypeNodeIndex() *LogConfigTypeNodeIndex {
	return &LogConfigTypeNodeIndex{
		pipeConfigs: make(map[string]*TypeNodePipeConfig),
	}
}

func (index *LogConfigTypeNodeIndex) GetConfig(logConfigKey string) ([]pipeline.Config, bool) {
	index.mutex.RLock()
	defer index.mutex.RUnlock()

	cfg, ok := index.pipeConfigs[logConfigKey]
	if !ok {
		return nil, false
	}
	return cfg.Raw, true
}

func (index *LogConfigTypeNodeIndex) DeleteConfig(logConfigKey string) bool {
	index.mutex.Lock()
	defer index.mutex.Unlock()

	_, ok := index.GetConfig(logConfigKey)
	if !ok {
		return false
	}
	delete(index.pipeConfigs, logConfigKey)
	return true
}

func (index *LogConfigTypeNodeIndex) SetConfig(logConfigKey string, p []pipeline.Config, lgc *v1beta1.LogConfig) {
	index.mutex.Lock()
	defer index.mutex.Unlock()

	index.pipeConfigs[logConfigKey] = &TypeNodePipeConfig{
		Raw: p,
		Lgc: lgc,
	}
}

func (index *LogConfigTypeNodeIndex) ValidateAndSetConfig(logConfigKey string, p []pipeline.Config, lgc *v1beta1.LogConfig) error {
	index.SetConfig(logConfigKey, p, lgc)
	if err := index.GetAll().ValidateUniquePipeName(); err != nil {
		index.DeleteConfig(logConfigKey)
		return err
	}
	return nil
}

func (index *LogConfigTypeNodeIndex) GetAll() *control.PipelineConfig {
	index.mutex.RLock()
	defer index.mutex.RUnlock()

	var cfgRaws []pipeline.Config
	for _, v := range index.pipeConfigs {
		cfgRaws = append(cfgRaws, v.Raw...)
	}
	all := &control.PipelineConfig{
		Pipelines: cfgRaws,
	}
	return all
}

func (index *LogConfigTypeNodeIndex) GetAllConfigMap() map[string]*TypeNodePipeConfig {
	index.mutex.RLock()
	defer index.mutex.RUnlock()

	return index.pipeConfigs
}
