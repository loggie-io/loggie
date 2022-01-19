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
	"github.com/loggie-io/loggie/pkg/pipeline"
)

type LogConfigTypeClusterIndex struct {
	pipeConfigs map[string][]pipeline.ConfigRaw // key: logConfigNamespace/Name, value: pipeline configs
}

func NewLogConfigTypeLoggieIndex() *LogConfigTypeClusterIndex {
	return &LogConfigTypeClusterIndex{
		pipeConfigs: make(map[string][]pipeline.ConfigRaw),
	}
}

func (index *LogConfigTypeClusterIndex) GetConfig(logConfigKey string) ([]pipeline.ConfigRaw, bool) {
	cfg, ok := index.pipeConfigs[logConfigKey]
	return cfg, ok
}

func (index *LogConfigTypeClusterIndex) DeleteConfig(logConfigKey string) bool {
	_, ok := index.GetConfig(logConfigKey)
	if !ok {
		return false
	}
	delete(index.pipeConfigs, logConfigKey)
	return true
}

func (index *LogConfigTypeClusterIndex) SetConfig(logConfigKey string, p []pipeline.ConfigRaw) {
	index.pipeConfigs[logConfigKey] = p
}

func (index *LogConfigTypeClusterIndex) ValidateAndSetConfig(logConfigKey string, p []pipeline.ConfigRaw) error {
	index.SetConfig(logConfigKey, p)
	if err := index.GetAll().Validate(); err != nil {
		index.DeleteConfig(logConfigKey)
		return err
	}
	return nil
}

func (index *LogConfigTypeClusterIndex) GetAll() *control.PipelineRawConfig {
	var cfgRaws []pipeline.ConfigRaw
	for _, v := range index.pipeConfigs {
		cfgRaws = append(cfgRaws, v...)
	}
	all := &control.PipelineRawConfig{
		Pipelines: cfgRaws,
	}
	return all
}
