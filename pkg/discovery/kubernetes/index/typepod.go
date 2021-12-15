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
	"k8s.io/apimachinery/pkg/util/sets"
	"loggie.io/loggie/pkg/control"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/discovery/kubernetes/helper"
	"loggie.io/loggie/pkg/pipeline"
)

type LogConfigTypePodIndex struct {
	pipeConfigs  map[string]*pipeline.ConfigRaw // key: podKey/lgcKey, value: related pipeline configs
	lgcToPodSets map[string]sets.String         // key: lgcKey(namespace/lgcName) , value: podKey(namespace/podName)
	podToLgcSets map[string]sets.String         // key: podKey(namespace/podName), value: lgcKey(namespace/lgcName)
}

func NewLogConfigTypePodIndex() *LogConfigTypePodIndex {
	return &LogConfigTypePodIndex{
		pipeConfigs:  make(map[string]*pipeline.ConfigRaw),
		lgcToPodSets: make(map[string]sets.String),
		podToLgcSets: make(map[string]sets.String),
	}
}

func (p *LogConfigTypePodIndex) GetPipeConfigs(namespace string, podName string, lgcName string) *pipeline.ConfigRaw {
	podKey := helper.MetaNamespaceKey(namespace, podName)
	lgcKey := helper.MetaNamespaceKey(namespace, lgcName)
	podAndLgc := helper.MetaNamespaceKey(podKey, lgcKey)
	cfgs, ok := p.pipeConfigs[podAndLgc]
	if !ok {
		return nil
	}

	return cfgs
}

func (p *LogConfigTypePodIndex) IsPodExist(namespace string, podName string) bool {
	podKey := helper.MetaNamespaceKey(namespace, podName)

	lgcSets, ok := p.podToLgcSets[podKey]
	if !ok || lgcSets.Len() == 0 {
		return false
	}
	return true
}

func (p *LogConfigTypePodIndex) GetPipeConfigsByPod(namespace string, podName string) []pipeline.ConfigRaw {
	podKey := helper.MetaNamespaceKey(namespace, podName)

	pipcfgs := make([]pipeline.ConfigRaw, 0)
	lgcSets, ok := p.podToLgcSets[podKey]
	if !ok || lgcSets.Len() == 0 {
		return pipcfgs
	}
	for _, lgcKey := range lgcSets.List() {
		podAndLgc := helper.MetaNamespaceKey(podKey, lgcKey)
		pipcfgs = append(pipcfgs, *p.pipeConfigs[podAndLgc])
	}
	return pipcfgs
}

func (p *LogConfigTypePodIndex) SetConfigs(namespace string, podName string, lgcName string, cfg *pipeline.ConfigRaw) {
	podKey := helper.MetaNamespaceKey(namespace, podName)
	lgcKey := helper.MetaNamespaceKey(namespace, lgcName)
	podAndLgc := helper.MetaNamespaceKey(podKey, lgcKey)

	p.pipeConfigs[podAndLgc] = cfg
	if _, ok := p.lgcToPodSets[lgcKey]; !ok {
		p.lgcToPodSets[lgcKey] = sets.NewString(podKey)
	} else {
		p.lgcToPodSets[lgcKey].Insert(podKey)
	}

	if _, ok := p.podToLgcSets[podKey]; !ok {
		p.podToLgcSets[podKey] = sets.NewString(lgcKey)
	} else {
		p.podToLgcSets[podKey].Insert(lgcKey)
	}
}

func (p *LogConfigTypePodIndex) ValidateAndSetConfigs(namespace string, podName string, lgcName string, cfg *pipeline.ConfigRaw) error {
	p.SetConfigs(namespace, podName, lgcName, cfg)
	if err := p.GetAllGroupByLogConfig().Validate(); err != nil {
		lgcKey := helper.MetaNamespaceKey(namespace, lgcName)
		p.DeletePipeConfigsByLogConfigKey(lgcKey)
		return err
	}
	return nil
}

func (p *LogConfigTypePodIndex) DeletePipeConfigsByLogConfigKey(lgcKey string) bool {

	// find lgc related pods
	podSets, ok := p.lgcToPodSets[lgcKey]
	if !ok || podSets.Len() == 0 {
		return false
	}

	for _, podKey := range podSets.List() {
		podAndLgc := helper.MetaNamespaceKey(podKey, lgcKey)
		delete(p.pipeConfigs, podAndLgc)

		lgcSets, ok := p.podToLgcSets[podKey]
		if ok {
			lgcSets.Delete(lgcKey)
		}
		if lgcSets.Len() == 0 {
			delete(p.podToLgcSets, podKey)
		}
	}

	delete(p.lgcToPodSets, lgcKey)

	return true
}

func (p *LogConfigTypePodIndex) DeletePipeConfigsByPodKey(podKey string) bool {
	// find pod related lgc sets
	lgcSets, ok := p.podToLgcSets[podKey]
	if !ok || lgcSets.Len() == 0 {
		return false
	}

	for _, lgcKey := range lgcSets.List() {
		podAndLgc := helper.MetaNamespaceKey(podKey, lgcKey)
		delete(p.pipeConfigs, podAndLgc)

		podSets, ok := p.lgcToPodSets[lgcKey]
		if ok {
			podSets.Delete(podKey)
		}
		if podSets.Len() == 0 {
			delete(p.lgcToPodSets, podKey)
		}
	}

	delete(p.podToLgcSets, podKey)
	return true
}

func (p *LogConfigTypePodIndex) GetAll() *control.PipelineRawConfig {
	conf := control.PipelineRawConfig{}
	var pipeConfigs []pipeline.ConfigRaw
	for _, cfg := range p.pipeConfigs {
		pipeConfigs = append(pipeConfigs, *cfg)
	}
	conf.Pipelines = pipeConfigs
	return &conf
}

func (p *LogConfigTypePodIndex) GetAllGroupByLogConfig() *control.PipelineRawConfig {
	conf := control.PipelineRawConfig{}
	var pipeConfigs []pipeline.ConfigRaw

	for lgcKey, podSet := range p.lgcToPodSets {
		cfg := pipeline.ConfigRaw{}
		for _, podKey := range podSet.List() {
			key := helper.MetaNamespaceKey(podKey, lgcKey)
			cfgRaw, ok := p.pipeConfigs[key]
			if !ok {
				log.Error("%s/%s is not in logConfigTypePodIndex", lgcKey, podKey)
				continue
			}
			cfg.Name = cfgRaw.Name
			cfg.Sources = append(cfg.Sources, cfgRaw.Sources...)
			cfg.Interceptors = cfgRaw.Interceptors
			cfg.Sink = cfgRaw.Sink
		}
		pipeConfigs = append(pipeConfigs, cfg)
	}

	conf.Pipelines = pipeConfigs
	return &conf
}
