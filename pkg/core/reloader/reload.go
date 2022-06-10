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
	"os"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/loggie-io/loggie/pkg/control"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/sink"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

type Reloader struct {
	controller *control.Controller

	config *ReloadConfig
	Done   chan struct{}
}

type ReloadConfig struct {
	Enabled      bool          `yaml:"enabled"`
	ConfigPath   string        `yaml:"-"`
	ReloadPeriod time.Duration `yaml:"period" default:"10s"`
}

func NewReloader(controller *control.Controller, config *ReloadConfig) *Reloader {
	reload := &Reloader{
		controller: controller,
		config:     config,
	}
	reload.initHttp()
	return reload
}

func (r *Reloader) Run(stopCh <-chan struct{}) {
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

			// read and validate config files
			newConfig, err := control.ReadPipelineConfigFromFile(r.config.ConfigPath, func(s os.FileInfo) bool {
				if time.Since(s.ModTime()) > 6*r.config.ReloadPeriod {
					return true
				}
				return false
			})
			if newConfig == nil || newConfig.Pipelines == nil {
				continue
			}

			if err != nil && !os.IsNotExist(err) {
				log.Error("read pipeline config file error: %v", err)
				continue
			}

			// diff config
			stopList, startList := diffConfig(newConfig, r.controller.CurrentConfig)

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

				eventbus.Publish(eventbus.ReloadTopic, eventbus.ReloadMetricData{
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

func diffConfig(newConfig *control.PipelineConfig, oldConfig *control.PipelineConfig) (stopComponentList []pipeline.Config, startComponentList []pipeline.Config) {
	oldPipeIndex := make(map[string]pipeline.Config)
	for _, p := range oldConfig.Pipelines {
		oldPipeIndex[p.Name] = p
	}

	stopList := make([]pipeline.Config, 0)
	startList := make([]pipeline.Config, 0)

	sourceComparer := cmp.Comparer(func(i, j []source.Config) bool {
		return cmp.Equal(i, j, cmpopts.SortSlices(func(a, b source.Config) bool {
			if a.Name > b.Name {
				return true
			}
			return false
		}))
	})
	interceptorComparer := cmp.Comparer(func(i, j []interceptor.Config) bool {
		return cmp.Equal(i, j, cmpopts.SortSlices(func(a, b interceptor.Config) bool {
			if a.Type > b.Type {
				return true
			}
			return false
		}))
	})
	sinkComparer := cmp.Comparer(func(i, j []sink.Config) bool {
		return cmp.Equal(i, j, cmpopts.SortSlices(func(a, b sink.Config) bool {
			if a.Name > b.Name {
				return true
			}
			return false
		}))
	})

	for _, newPipe := range newConfig.Pipelines {
		oldPipe, ok := oldPipeIndex[newPipe.Name]
		if !ok {
			// pipeline is new
			startList = append(startList, newPipe)
			continue
		}
		delete(oldPipeIndex, oldPipe.Name)

		// diff
		equal := cmp.Equal(newPipe, oldPipe, sourceComparer, interceptorComparer, sinkComparer)
		if !equal {
			startList = append(startList, newPipe)
			stopList = append(stopList, oldPipe)
		}
		if !equal && log.IsDebugLevel() {
			diff := cmp.Diff(newPipe, oldPipe, sourceComparer, interceptorComparer, sinkComparer)
			log.Debug("diff pipeline config: \n%s", diff)
		}
	}

	// add old pipelines to stopList
	for k := range oldPipeIndex {
		stopList = append(stopList, oldPipeIndex[k])
	}

	return stopList, startList
}
