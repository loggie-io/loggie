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
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/external"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	"github.com/loggie-io/loggie/pkg/pipeline"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	OverrideLogConfigAnnotation        = "logconfig.loggie.io/override"
	OverrideClusterLogConfigAnnotation = "clusterlogconfig.loggie.io/override"

	K8sFieldsKey = "@privateK8sFields"
)

type LogConfigTypePodIndex struct {
	pipeConfigs  map[string]*TypePodPipeConfig // key: podKey/lgcKey, value: related pipeline configs
	lgcToPodSets map[string]sets.String        // key: lgcKey(namespace/lgcName), value: podKey(namespace/podName)
	podToLgcSets map[string]sets.String        // key: podKey(namespace/podName), value: lgcKey(namespace/lgcName)

	lgcToOverrideLgc map[string]string // key: lgcKey(namespace/lgcName), value: override lgcKey(namespace/lgcName)
}

type TypePodPipeConfig struct {
	Raw *pipeline.Config
	Lgc *v1beta1.LogConfig
	// ContainerIds is used to check whether containers in the Pod have been restarted or crashed.
	// If the containerId changes, the log path needs to be generated again
	ContainerIds sets.String
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
	podKey := helper.MetaNamespaceKey(namespace, podName)
	lgcKey := helper.MetaNamespaceKey(lgcNamespace, lgcName)
	podAndLgc := helper.MetaNamespaceKey(podKey, lgcKey)
	cfgs, ok := p.pipeConfigs[podAndLgc]
	if !ok {
		return nil
	}

	return cfgs.Raw
}

// IsPodUpdated check weather pod is in the index or pod containers has changed.
func (p *LogConfigTypePodIndex) IsPodUpdated(pod *corev1.Pod) bool {
	podKey := helper.MetaNamespaceKey(pod.Namespace, pod.Name)

	lgcSets, ok := p.podToLgcSets[podKey]
	if !ok || lgcSets.Len() == 0 {
		return true
	}

	// pod is in the index, then check the container id
	anyLgcKey := lgcSets.List()[0]
	podAndLgc := helper.MetaNamespaceKey(podKey, anyLgcKey)
	cfgs, ok := p.pipeConfigs[podAndLgc]
	if !ok {
		return true
	}

	newContainerIds := helper.GetContainerIds(pod)
	return !newContainerIds.Equal(cfgs.ContainerIds)
}

func (p *LogConfigTypePodIndex) SetConfigs(pod *corev1.Pod, lgcName string, cfg *pipeline.Config, lgc *v1beta1.LogConfig) {
	podKey := helper.MetaNamespaceKey(pod.Namespace, pod.Name)
	lgcKey := helper.MetaNamespaceKey(lgc.Namespace, lgcName)
	podAndLgc := helper.MetaNamespaceKey(podKey, lgcKey)

	if overrideLgc := overrideLgcKey(lgc); overrideLgc != "" {
		p.lgcToOverrideLgc[lgcKey] = overrideLgc
	}

	p.pipeConfigs[podAndLgc] = &TypePodPipeConfig{
		Raw:          cfg,
		Lgc:          lgc,
		ContainerIds: helper.GetContainerIds(pod),
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
		if !ok {
			continue
		}
		lgcSets.Delete(lgcKey)
		if lgcSets.Len() == 0 {
			delete(p.podToLgcSets, podKey)
		}
	}

	delete(p.lgcToPodSets, lgcKey)
	delete(p.lgcToOverrideLgc, lgcKey)
	external.DelDynamicPaths(lgcKey)

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
		if !ok {
			continue
		}
		podSets.Delete(podKey)
		if podSets.Len() == 0 {
			delete(p.lgcToPodSets, lgcKey)
			external.DelDynamicPaths(lgcKey)
		}
	}

	delete(p.podToLgcSets, podKey)
	return true
}

func (p *LogConfigTypePodIndex) GetAllConfigMap() map[string]*TypePodPipeConfig {
	return p.pipeConfigs
}

func (p *LogConfigTypePodIndex) GetAllGroupByLogConfig(dynamicContainerLog bool) *control.PipelineConfig {
	conf := control.PipelineConfig{}
	var pipeConfigs []pipeline.Config

	// Merge sources associated with multiple pods into one.
	for lgcKey, podSet := range p.lgcToPodSets {
		if podSet.Len() == 0 {
			continue
		}

		pipe := p.mergePodsSources(dynamicContainerLog, lgcKey, podSet.List())
		if pipe.Name != "" {
			pipeConfigs = append(pipeConfigs, pipe)
		}
	}

	conf.SetPipelines(pipeConfigs)
	return &conf
}

func (p *LogConfigTypePodIndex) mergePodsSources(dynamicContainerLog bool, lgcKey string, pods []string) pipeline.Config {
	ignoredKeys := p.IgnoredPodKeyAndLgcKeys()

	if dynamicContainerLog {
		return p.getDynamicPipelines(lgcKey, pods, ignoredKeys)
	}

	aggCfg := pipeline.Config{}
	icpSets := make(map[string]*interceptor.Config)

	for _, podKey := range pods {
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

		// queue is same
		aggCfg.Queue = cfgRaw.Raw.Queue

		// in normal, interceptor is same, but we may need to append interceptor.belongTo
		mergeInterceptors(icpSets, cfgRaw.Raw.Interceptors)
	}
	icpList := interceptorSetsToList(icpSets)
	aggCfg.Interceptors = icpList

	return aggCfg
}

func (p *LogConfigTypePodIndex) getDynamicPipelines(lgcKey string, pods []string, ignoredKeys map[string]struct{}) pipeline.Config {
	aggCfg := pipeline.Config{}

	latestPodPipeline := &pipeline.Config{}
	var allPodSource []*source.Config
	for _, podKey := range pods {
		key := helper.MetaNamespaceKey(podKey, lgcKey)
		cfgRaw, ok := p.pipeConfigs[key]
		if !ok {
			log.Error("%s/%s is not in logConfigTypePodIndex", lgcKey, podKey)
			continue
		}
		latestPodPipeline = cfgRaw.Raw

		if _, ok := ignoredKeys[key]; ok {
			log.Debug("%s is ignored because it's override by other logConfigs", key)
			continue
		}

		allPodSource = append(allPodSource, cfgRaw.Raw.Sources...)
	}
	// set dynamic paths and k8s fields
	setDynamicSourcePaths(allPodSource, lgcKey)

	var srcCopyList []*source.Config
	uniqSourceName := make(map[string]struct{})
	for _, src := range latestPodPipeline.Sources {
		originSourceName := helper.GetTypePodOriginSourceName(src.Name)

		// When there are multiple containers in the pod, source name would be same, so we need to be deduplicated.
		if _, ok := uniqSourceName[originSourceName]; ok {
			continue
		}
		uniqSourceName[originSourceName] = struct{}{}

		// In order to avoid modifying the original source, we need to deep copy it here
		srcCopy := src.DeepCopy()
		srcCopy.Name = originSourceName
		// set paths to containerLog
		helper.SetPathsToSource(srcCopy, []string{external.SystemContainerLogsPath})

		delete(srcCopy.Fields, K8sFieldsKey)
		srcCopyList = append(srcCopyList, srcCopy)
	}

	aggCfg.Sources = srcCopyList
	aggCfg.Name = latestPodPipeline.Name
	aggCfg.Interceptors = latestPodPipeline.Interceptors
	aggCfg.Sink = latestPodPipeline.Sink
	aggCfg.Queue = latestPodPipeline.Queue
	return aggCfg
}

func setDynamicSourcePaths(sourceConfigs []*source.Config, pipelineName string) {
	if len(sourceConfigs) == 0 {
		return
	}

	srcNameToPair := map[string][]external.PathFieldsPair{}
	for _, src := range sourceConfigs {
		// set dynamic paths
		pair := external.PathFieldsPair{}
		paths := helper.GetPathsFromSource(src)
		pair.Paths = paths

		if k8sFields, ok := src.Fields[K8sFieldsKey]; ok {
			fields := k8sFields.(map[string]interface{})
			pair.Fields = fields
		}

		originName := helper.GetTypePodOriginSourceName(src.Name)
		if pairs, ok := srcNameToPair[originName]; ok {
			pairs = append(pairs, pair)
			srcNameToPair[originName] = pairs
		} else {
			srcNameToPair[originName] = []external.PathFieldsPair{pair}
		}
	}

	for srcName, pairs := range srcNameToPair {
		external.SetDynamicPaths(pipelineName, srcName, pairs)
	}
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

func mergeInterceptors(icpSets map[string]*interceptor.Config, interceptors []*interceptor.Config) {
	for _, icp := range interceptors {
		icpVal, ok := icpSets[icp.UID()]
		if !ok {
			icpSets[icp.UID()] = icp
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

func interceptorSetsToList(icpSets map[string]*interceptor.Config) []*interceptor.Config {
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
