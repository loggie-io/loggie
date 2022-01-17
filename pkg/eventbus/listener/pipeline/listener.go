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

package pipeline

import (
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/eventbus"
	"strings"
	"time"
)

func init() {
	eventbus.Registry(makeListener(), eventbus.WithTopics([]string{eventbus.PipelineTopic, eventbus.ComponentBaseTopic}))
}

func makeListener() *Listener {
	l := &Listener{
		done:                 make(chan struct{}),
		config:               &Config{},
		eventChan:            make(chan eventbus.Event),
		namePipelineMetric:   make(map[string]eventbus.PipelineMetricData),
		nameComponentMetrics: make(map[string][]eventbus.ComponentBaseMetricData),
	}
	return l
}

type Config struct {
	Period           time.Duration `yaml:"period" default:"30s"`
	ValidateDuration time.Duration `yaml:"validateDuration" default:"60s"`
}

type Listener struct {
	config               *Config
	done                 chan struct{}
	eventChan            chan eventbus.Event
	namePipelineMetric   map[string]eventbus.PipelineMetricData
	nameComponentMetrics map[string][]eventbus.ComponentBaseMetricData
}

func (l *Listener) Name() string {
	return "pipeline"
}

func (l *Listener) Init(ctx api.Context) {
}

func (l *Listener) Start() {
	go l.run()
}

func (l *Listener) Stop() {
	close(l.done)
}

func (l *Listener) Subscribe(event eventbus.Event) {
	l.eventChan <- event
}

func (l *Listener) Config() interface{} {
	return l.config
}

func (l *Listener) run() {
	tick := time.Tick(l.config.Period)
	for {
		select {
		case <-l.done:
			return
		case e := <-l.eventChan:
			l.consumer(e)
		case <-tick:
			l.validate()
		}
	}
}

func (l *Listener) consumer(event eventbus.Event) {
	if event.Topic == eventbus.PipelineTopic {
		d, ok := event.Data.(eventbus.PipelineMetricData)
		if !ok {
			log.Panic("convert eventbus reload failed: %v", event)
		}
		if _, exist := l.namePipelineMetric[d.Name]; exist {
			delete(l.nameComponentMetrics, d.Name)
		}
		l.namePipelineMetric[d.Name] = d
	}
	if event.Topic == eventbus.ComponentBaseTopic {
		d, ok := event.Data.(eventbus.ComponentBaseMetricData)
		if !ok {
			log.Panic("convert eventbus reload failed: %v", event)
		}
		if _, exist := l.nameComponentMetrics[d.PipelineName]; !exist {
			l.nameComponentMetrics[d.PipelineName] = make([]eventbus.ComponentBaseMetricData, 0)
		}
		componentBaseMetricDataArray := l.nameComponentMetrics[d.PipelineName]

		for i, baseMetricData := range componentBaseMetricDataArray {
			if d.Config.Name != baseMetricData.Config.Name {
				continue
			}
			if d.Config.Type != baseMetricData.Config.Type {
				continue
			}
			if d.EventType != baseMetricData.EventType {
				continue
			}
			// remove same component event
			componentBaseMetricDataArray = append(componentBaseMetricDataArray[:i], componentBaseMetricDataArray[i+1:]...)
		}
		componentBaseMetricDataArray = append(componentBaseMetricDataArray, d)
		l.nameComponentMetrics[d.PipelineName] = componentBaseMetricDataArray
	}
}

func (l *Listener) validate() {
	if len(l.namePipelineMetric) <= 0 {
		return
	}
	for _, pipelineMetricData := range l.namePipelineMetric {
		if time.Since(pipelineMetricData.Time) < l.config.ValidateDuration {
			continue
		}
		pipelineName := pipelineMetricData.Name
		componentBaseMetricData, exist := l.nameComponentMetrics[pipelineName]
		if !exist {
			log.Error("pipeline(%s) %s timeout, because no component event", pipelineName, pipelineMetricData.EventType)
			continue
		}
		reportPipelineComponentConfig := make([]eventbus.ComponentBaseConfig, 0)
		for _, baseMetricData := range componentBaseMetricData {
			if baseMetricData.EventType == pipelineMetricData.EventType {
				reportPipelineComponentConfig = append(reportPipelineComponentConfig, baseMetricData.Config)
			}
		}
		missComponentConfig := make([]eventbus.ComponentBaseConfig, 0)
		for _, componentConfig := range pipelineMetricData.ComponentConfigs {
			if !contain(componentConfig, reportPipelineComponentConfig) {
				missComponentConfig = append(missComponentConfig, componentConfig)
			}
		}
		if len(missComponentConfig) > 0 {
			log.Error("pipeline(%s) %s timeout, because miss component: %s", pipelineName, pipelineMetricData.EventType, componentConfigInfo(missComponentConfig))
			for _, datum := range componentBaseMetricData {
				if datum.EventType == pipelineMetricData.EventType {
					log.Warn("%s pipeline(%s) current metric component: %+v", pipelineMetricData.EventType, pipelineName, datum.Config)
				}
			}
		}
		// clean data
		delete(l.namePipelineMetric, pipelineName)
		delete(l.nameComponentMetrics, pipelineName)
	}
}

func componentConfigInfo(componentConfigs []eventbus.ComponentBaseConfig) string {
	var info strings.Builder
	for _, config := range componentConfigs {
		info.WriteString("{")
		info.WriteString(config.Code())
		info.WriteString("}")
	}
	return info.String()
}

func contain(target eventbus.ComponentBaseConfig, array []eventbus.ComponentBaseConfig) bool {
	for _, config := range array {
		if target.Code() == config.Code() {
			return true
		}
	}
	return false
}
