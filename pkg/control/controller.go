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
	"github.com/goccy/go-yaml"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

const handleCurrentPipelines = "/api/v1/controller/pipelines"

func (c *Controller) initHttp() {
	http.HandleFunc(handleCurrentPipelines, c.currentPipelinesHandler)
}

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
	c.initHttp()
	c.StartPipelines(config.Pipelines)
}

func (c *Controller) StartPipelines(configs []pipeline.Config) {
	// add new pipeline configs to currentConfig
	c.CurrentConfig.AddPipelines(configs)

	// start new pipelines
	for _, pConfig := range configs {
		p := pipeline.NewPipeline(&pConfig)
		log.Info("starting pipeline: %s", pConfig.Name)
		c.reportMetric(pConfig, eventbus.ComponentStart)
		if err := p.Start(); err != nil {
			log.Error("start pipeline error: %v", err)
			c.pipelineRunner[pConfig.Name] = p
			p.Stop()
			// We will retry when reload
			return
		}

		p.Running = true
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
		c.reportMetric(pConfig, eventbus.ComponentStop)
		p.Stop()
		delete(c.pipelineRunner, pConfig.Name)
	}
}

func (c *Controller) RetryNotRunningPipeline() {
	for _, p := range c.pipelineRunner {
		if p.Running {
			continue
		}

		if err := p.Start(); err != nil {
			log.Error("retry starting pipeline error: %v", err)
			p.Stop()
			continue
		}

		p.Running = true
	}
}

func (c *Controller) reportMetric(p pipeline.Config, eventType eventbus.ComponentEventType) {
	componentConfigs := make([]eventbus.ComponentBaseConfig, 0)
	// queue config
	componentConfigs = append(componentConfigs, eventbus.ComponentBaseConfig{
		Name:     p.Queue.ComponentBaseConfig.Name,
		Type:     api.Type(p.Queue.ComponentBaseConfig.Type),
		Category: api.QUEUE,
	})
	// sink config
	componentConfigs = append(componentConfigs, eventbus.ComponentBaseConfig{
		Name:     p.Sink.ComponentBaseConfig.Name,
		Type:     api.Type(p.Sink.ComponentBaseConfig.Type),
		Category: api.SINK,
	})
	// source config
	for _, s := range p.Sources {
		componentConfigs = append(componentConfigs, eventbus.ComponentBaseConfig{
			Name:     s.ComponentBaseConfig.Name,
			Type:     api.Type(s.ComponentBaseConfig.Type),
			Category: api.SOURCE,
		})
	}
	// interceptor config
	for _, i := range p.Interceptors {
		componentConfigs = append(componentConfigs, eventbus.ComponentBaseConfig{
			Name:     i.ComponentBaseConfig.Name,
			Type:     api.Type(i.ComponentBaseConfig.Type),
			Category: api.INTERCEPTOR,
		})
	}
	eventbus.Publish(eventbus.PipelineTopic, eventbus.PipelineMetricData{
		EventType:        eventType,
		Name:             p.Name,
		Time:             time.Now(),
		ComponentConfigs: componentConfigs,
	})
}

func (c *Controller) currentPipelinesHandler(writer http.ResponseWriter, request *http.Request) {
	data, err := yaml.Marshal(c.CurrentConfig)
	if err != nil {
		log.Warn("marshal current pipeline config err: %v", err)
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(err.Error()))
		return
	}

	writer.WriteHeader(http.StatusOK)
	writer.Write(data)
}
