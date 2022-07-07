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
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	OverrideLogConfigAnnotation        = "logconfig.loggie.io/override"
	OverrideClusterLogConfigAnnotation = "clusterlogconfig.loggie.io/override"
)

type LogConfigTypePodIndex struct {
	pipeConfigs  map[string]*TypePodPipeConfig // key: podKey/lgcKey, value: related pipeline configs
	lgcToPodSets map[string]sets.String        // key: lgcKey(namespace/lgcName), value: podKey(namespace/podName)
	podToLgcSets map[string]sets.String        // key: podKey(namespace/podName), value: lgcKey(namespace/lgcName)

	lgcToOverrideLgc map[string]string // key: lgcKey(namespace/lgcName), value: override lgcKey(namespace/lgcName)
}

type TypePodPipeConfig struct {
	Raw *pipeline.ConfigRaw
	Lgc *v1beta1.LogConfig
}

func NewLogConfigTypePodIndex() *LogConfigTypePodIndex {
	return &LogConfigTypePodIndex{
		pipeConfigs:      make(map[string]*TypePodPipeConfig),
		lgcToPodSets:     make(map[string]sets.String),
		podToLgcSets:     make(map[string]sets.String),
		lgcToOverrideLgc: make(map[string]string),
	}
}

func (p *LogConfigTypePodIndex) GetPipeConfigs(namespace string, podName string, lgcNamespace string, lgcName string) *pipeline.ConfigRaw {
	podKey := helper.MetaNamespaceKey(namespace, podName)
	lgcKey := helper.MetaNamespaceKey(lgcNamespace, lgcName)
	podAndLgc := helper.MetaNamespaceKey(podKey, lgcKey)
	cfgs, ok := p.pipeConfigs[podAndLgc]
	if !ok {
		return nil
	}

	return cfgs.Raw
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
		pipcfgs = append(pipcfgs, *p.pipeConfigs[podAndLgc].Raw)
	}
	return pipcfgs
}

func (p *LogConfigTypePodIndex) SetConfigs(namespace string, podName string, lgcNamespace string, lgcName string, cfg *pipeline.ConfigRaw, lgc *v1beta1.LogConfig) {
	podKey := helper.MetaNamespaceKey(namespace, podName)
	lgcKey := helper.MetaNamespaceKey(lgcNamespace, lgcName)
	podAndLgc := helper.MetaNamespaceKey(podKey, lgcKey)

	if overrideLgc := overrideLgcKey(lgc); overrideLgc != "" {
		p.lgcToOverrideLgc[lgcKey] = overrideLgc
	}

	p.pipeConfigs[podAndLgc] = &TypePodPipeConfig{
		Raw: cfg,
		Lgc: lgc,
	}
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

func (p *LogConfigTypePodIndex) ValidateAndSetConfigs(namespace string, podName string, lgcNamespace string, lgcName string,
	cfg *pipeline.ConfigRaw, lgc *v1beta1.LogConfig) error {
	p.SetConfigs(namespace, podName, lgcNamespace, lgcName, cfg, lgc)
	if err := p.GetAllGroupByLogConfig().ValidateUniquePipeName(); err != nil {
		if namespace == "" {
			log.Warn("validate clusterLogConfig error: %v", err)
		} else {
			log.Warn("validate logConfig error: %v", err)
		}
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
	delete(p.lgcToOverrideLgc, lgcKey)

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

func (p *LogConfigTypePodIndex) GetAllConfigMap() map[string]*TypePodPipeConfig {
	return p.pipeConfigs
}

type ExtInterceptorConfig struct {
	interceptor.ExtensionConfig `yaml:",inline,omitempty"`
	cfg.CommonCfg               `yaml:",inline,omitempty"`
}

func (p *LogConfigTypePodIndex) GetAllGroupByLogConfig() *control.PipelineRawConfig {
	conf := control.PipelineRawConfig{}
	var pipeConfigs []pipeline.ConfigRaw

	ignoredKeys := p.IgnoredPodKeyAndLgcKeys()

	for lgcKey, podSet := range p.lgcToPodSets {
		if podSet.Len() == 0 {
			continue
		}
		aggCfg := pipeline.ConfigRaw{}
		icpSets := make(map[string]ExtInterceptorConfig)

		for _, podKey := range podSet.List() {
			key := helper.MetaNamespaceKey(podKey, lgcKey)
			cfgRaw, ok := p.pipeConfigs[key]
			if !ok {
				log.Error("%s/%s is not in logConfigTypePodIndex", lgcKey, podKey)
				continue
			}

			if _, ok := ignoredKeys[key]; ok {
				log.Debug("%s is ignored because it's override by other logConfigs", key)
				continue
			}

			aggCfg.Name = cfgRaw.Raw.Name
			aggCfg.Sources = append(aggCfg.Sources, cfgRaw.Raw.Sources...)
			aggCfg.Sink = cfgRaw.Raw.Sink

			// merge interceptor.belongTo
			mergeInterceptors(icpSets, cfgRaw.Raw.Interceptors)
		}
		icpList := extInterceptorToCommonCfg(icpSets)
		aggCfg.Interceptors = icpList

		if aggCfg.Name != "" {
			pipeConfigs = append(pipeConfigs, aggCfg)
		}
	}

	conf.Pipelines = pipeConfigs
	return &conf
}

// IgnoredPodKeyAndLgcKeys return map which key is podKey/overrideLgcKey
func (p *LogConfigTypePodIndex) IgnoredPodKeyAndLgcKeys() map[string]struct{} {
	ignored := make(map[string]struct{})

	if len(p.lgcToOverrideLgc) == 0 {
		return ignored
	}

	for lgcKey, overrideLgcKey := range p.lgcToOverrideLgc {
		// get lgc pod keys
		podSets, ok := p.lgcToPodSets[lgcKey]
		if !ok {
			continue
		}

		for _, podKey := range podSets.List() {
			key := helper.MetaNamespaceKey(podKey, overrideLgcKey)
			ignored[key] = struct{}{}
		}
	}

	return ignored
}

func mergeInterceptors(icpSets map[string]ExtInterceptorConfig, interceptors []cfg.CommonCfg) {
	for _, icp := range interceptors {

		extIcp := &ExtInterceptorConfig{}
		if err := cfg.Unpack(icp, extIcp); err != nil {
			log.Warn("unpack interceptor config error: %+v", err)
			continue
		}

		icpVal, ok := icpSets[extIcp.UID()]
		if !ok {
			icpSets[extIcp.UID()] = *extIcp
			continue
		}

		if len(extIcp.BelongTo) == 0 {
			continue
		}

		// merge when belongTo exist
		icpVal.BelongTo = append(icpVal.BelongTo, extIcp.BelongTo...)
		icpSets[extIcp.UID()] = icpVal
	}
}

func extInterceptorToCommonCfg(icpSets map[string]ExtInterceptorConfig) []cfg.CommonCfg {
	icpList := make([]cfg.CommonCfg, 0)
	for _, v := range icpSets {
		c, err := cfg.Pack(v)
		if err != nil {
			log.Info("pack interceptor config error: %+v", err)
			continue
		}
		icpList = append(icpList, c)
	}
	return icpList
}

// overrideLgcKey return namespace/logConfigName or /clusterLogConfigName
// retrieve from OverrideLogConfigAnnotation or OverrideClusterLogConfigAnnotation
func overrideLgcKey(in *v1beta1.LogConfig) string {
	if overrideLgc, ok := in.Annotations[OverrideLogConfigAnnotation]; ok {
		return helper.MetaNamespaceKey(in.Namespace, overrideLgc)
	}

	if overrideClusterLgc, ok := in.Annotations[OverrideClusterLogConfigAnnotation]; ok {
		return helper.MetaNamespaceKey("", overrideClusterLgc)
	}

	return ""
}
