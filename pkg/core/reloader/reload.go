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

package reloader

import (
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/util/yaml"
	"github.com/pkg/errors"
	"os"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/loggie-io/loggie/pkg/control"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

var globalReloader *reloader

type reloader struct {
	controller *control.Controller

	config *ReloadConfig
	Done   chan struct{}
}

type ReloadConfig struct {
	Enabled      bool          `yaml:"enabled"`
	ConfigPath   string        `yaml:"-"`
	ReloadPeriod time.Duration `yaml:"period" default:"10s"`
}

func Setup(stopCh <-chan struct{}, controller *control.Controller, config *ReloadConfig) {
	reload := &reloader{
		controller: controller,
		config:     config,
	}
	reload.initHttp()
	globalReloader = reload
	go globalReloader.Run(stopCh)
}

func (r *reloader) Run(stopCh <-chan struct{}) {
	log.Info("reloader starting...")
	t := time.NewTicker(r.config.ReloadPeriod)
	defer t.Stop()
	for {
		select {
		case <-stopCh:
			log.Info("stop config reload")
			return

		case <-t.C:

			// If there is at least one pipeline not running, we will not ignore the configuration, and always try to restart the pipeline
			r.controller.RetryNotRunningPipeline()

			newConfig, _, stopList, startList := DiffPipelineConfigs(func(s os.FileInfo) bool {
				if time.Since(s.ModTime()) > 6*r.config.ReloadPeriod {
					return true
				}
				return false
			})

			if len(stopList) > 0 || len(startList) > 0 {
				log.Info("loggie is reloading..")

				if newConfig != nil {
					out, err := yaml.Marshal(newConfig)
					if err == nil {
						log.Info("reload latest pipelines config:\n%s", string(out))
					}
				}

				if log.IsDebugLevel() && r.controller.CurrentConfig != nil {
					if older, err := yaml.Marshal(r.controller.CurrentConfig); err == nil {
						log.Debug("older/current pipelines config:\n%s", string(older))
					}
				}

				eventbus.PublishOrDrop(eventbus.ReloadTopic, eventbus.ReloadMetricData{
					Tick: 1,
				})
			}

			if len(stopList) > 0 {
				r.controller.StopPipelines(stopList)
			}
			if len(startList) > 0 {
				r.controller.StartPipelines(startList)
			}
		}
	}
}

func DiffPipelineConfigs(ignoreFunc control.FileIgnore) (newCfg *control.PipelineConfig, diffPipes []string, stopComponentList []pipeline.Config, startComponentList []pipeline.Config) {
	// read and validate config files
	newConfig, err := control.ReadPipelineConfigFromFile(globalReloader.config.ConfigPath, ignoreFunc)
	if err != nil && !os.IsNotExist(err) {
		if errors.Is(err, control.ErrIgnoreAllFile) {
			return nil, nil, nil, nil
		}

		log.Error("read pipeline config file error: %v", err)
		return nil, nil, nil, nil
	}

	// Empty configuration cannot be ignored, because if it is empty configuration, it may be necessary to stop the current pipeline
	if newConfig == nil {
		newConfig = &control.PipelineConfig{}
	}

	// diff config
	diffs, stopList, startList := diffConfig(newConfig, globalReloader.controller.CurrentConfig)
	return newConfig, diffs, stopList, startList
}

func diffConfig(newConfig *control.PipelineConfig, oldConfig *control.PipelineConfig) (diffList []string, stopComponentList []pipeline.Config, startComponentList []pipeline.Config) {
	oldPipeIndex := make(map[string]pipeline.Config)
	for _, p := range oldConfig.GetPipelines() {
		oldPipeIndex[p.Name] = p
	}

	stopList := make([]pipeline.Config, 0)
	startList := make([]pipeline.Config, 0)

	sourceComparer := cmp.Comparer(func(i, j []*source.Config) bool {
		return cmp.Equal(i, j, cmpopts.SortSlices(func(a, b *source.Config) bool {
			if a.Name > b.Name {
				return true
			}
			return false
		}))
	})

	interceptorComparer := cmp.Comparer(func(i, j []*interceptor.Config) bool {
		return cmp.Equal(i, j, cmpopts.SortSlices(func(a, b *interceptor.Config) bool {
			if a.Type > b.Type {
				return true
			}
			return false
		}))
	})

	var diffs []string
	for _, newPipe := range newConfig.GetPipelines() {
		oldPipe, ok := oldPipeIndex[newPipe.Name]
		if !ok {
			// pipeline is new
			startList = append(startList, newPipe)
			continue
		}
		delete(oldPipeIndex, oldPipe.Name)

		// diff
		if cmp.Equal(oldPipe, newPipe, sourceComparer, interceptorComparer) {
			continue
		}

		startList = append(startList, newPipe)
		stopList = append(stopList, oldPipe)
		diff := cmp.Diff(oldPipe, newPipe, sourceComparer, interceptorComparer)
		log.Info("diff pipeline %s: \n%s", newPipe.Name, diff)
		diffs = append(diffs, diff)
	}

	// add old pipelines to stopList
	for k := range oldPipeIndex {
		stopList = append(stopList, oldPipeIndex[k])
	}

	return diffs, stopList, startList
}
