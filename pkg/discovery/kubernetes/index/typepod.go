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
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"k8s.io/apimachinery/pkg/util/sets"
	"sync"
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

	mutex sync.RWMutex
}

type TypePodPipeConfig struct {
	Raw *pipeline.Config
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

func (p *LogConfigTypePodIndex) GetPipeConfigs(namespace string, podName string, lgcNamespace string, lgcName string) *pipeline.Config {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

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
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	podKey := helper.MetaNamespaceKey(namespace, podName)

	lgcSets, ok := p.podToLgcSets[podKey]
	if !ok || lgcSets.Len() == 0 {
		return false
	}
	return true
}

func (p *LogConfigTypePodIndex) setConfigs(namespace string, podName string, lgcName string, cfg *pipeline.Config, lgc *v1beta1.LogConfig) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	podKey := helper.MetaNamespaceKey(namespace, podName)
	lgcKey := helper.MetaNamespaceKey(lgc.Namespace, lgcName)
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

func (p *LogConfigTypePodIndex) ValidateAndSetConfigs(namespace string, podName string, lgcName string,
	cfg *pipeline.Config, lgc *v1beta1.LogConfig) error {
	p.setConfigs(namespace, podName, lgcName, cfg, lgc)
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
	p.mutex.Lock()
	defer p.mutex.Unlock()

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
	p.mutex.Lock()
	defer p.mutex.Unlock()

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
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	return p.pipeConfigs
}

func (p *LogConfigTypePodIndex) GetAllGroupByLogConfig() *control.PipelineConfig {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	conf := control.PipelineConfig{}
	var pipeConfigs []pipeline.Config

	ignoredKeys := p.ignoredPodKeyAndLgcKeys()

	// merge logConfig of pods to one, reduce pipelines
	for lgcKey, podSet := range p.lgcToPodSets {
		if podSet.Len() == 0 {
			continue
		}
		aggCfg := pipeline.Config{}
		icpSets := make(map[string]*interceptor.Config)

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
			// append sources
			aggCfg.Sources = append(aggCfg.Sources, cfgRaw.Raw.Sources...)
			// sink is same
			aggCfg.Sink = cfgRaw.Raw.Sink

			// in normal, interceptor is same, but we may need to append interceptor.belongTo
			mergeInterceptors(icpSets, cfgRaw.Raw.Interceptors)
		}
		icpList := ipcSetsToList(icpSets)
		aggCfg.Interceptors = icpList

		if aggCfg.Name != "" {
			pipeConfigs = append(pipeConfigs, aggCfg)
		}
	}

	conf.Pipelines = pipeConfigs
	return &conf
}

// IgnoredPodKeyAndLgcKeys return map which key is podKey/overrideLgcKey
func (p *LogConfigTypePodIndex) ignoredPodKeyAndLgcKeys() map[string]struct{} {
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

func mergeInterceptors(icpSets map[string]*interceptor.Config, interceptors []*interceptor.Config) {
	for _, icp := range interceptors {
		icpVal, ok := icpSets[icp.UID()]
		if !ok {
			icpSets[icp.UID()] = icp
			continue
		}

		if !icp.HasBelongTo() {
			continue
		}

		ext, err := icp.GetExtension()
		if err != nil {
			log.Warn("get ExtensionConfig from interceptor failed: %+v", err)
			continue
		}
		if len(ext.BelongTo) == 0 {
			continue
		}

		oriExt, err := icpVal.GetExtension()
		if err != nil {
			log.Warn("get ExtensionConfig from interceptor failed: %+v", err)
			continue
		}

		// merge when `belongTo` exist
		oriExt.BelongTo = append(oriExt.BelongTo, ext.BelongTo...)
		icpVal.SetBelongTo(oriExt.BelongTo)
		icpSets[icp.UID()] = icpVal
	}
}

func ipcSetsToList(icpSets map[string]*interceptor.Config) []*interceptor.Config {
	icpList := make([]*interceptor.Config, 0)
	for _, v := range icpSets {
		icpList = append(icpList, v)
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
