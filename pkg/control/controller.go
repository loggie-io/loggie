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

package control

import (
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/pipeline"
	_ "net/http/pprof"
)

type Controller struct {
	CurrentConfig  *PipelineConfig
	pipelineRunner map[string]*pipeline.Pipeline
}

func NewController() *Controller {
	return &Controller{
		CurrentConfig:  &PipelineConfig{},
		pipelineRunner: make(map[string]*pipeline.Pipeline),
	}
}

func (c *Controller) Start(config *PipelineConfig) {
	c.StartPipelines(config.Pipelines)
}

func (c *Controller) StartPipelines(configs []pipeline.Config) {
	// add new pipeline configs to currentConfig
	c.CurrentConfig.AddPipelines(configs)

	// start new pipelines
	for _, pConfig := range configs {
		p := pipeline.NewPipeline()
		log.Info("starting pipeline: %s", pConfig.Name)
		p.Start(pConfig)

		c.pipelineRunner[pConfig.Name] = p
	}
}

func (c *Controller) StopPipelines(configs []pipeline.Config) {
	// remove pipeline configs from currentConfig
	c.CurrentConfig.RemovePipelines(configs)

	// stop pipelines
	for _, pConfig := range configs {
		p, ok := c.pipelineRunner[pConfig.Name]
		if !ok {
			continue
		}

		log.Info("stopping pipeline: %s", pConfig.Name)
		p.Stop()
	}
}
