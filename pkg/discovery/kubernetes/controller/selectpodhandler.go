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

package controller

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/runtime"
	"github.com/loggie-io/loggie/pkg/source/codec"
	"github.com/loggie-io/loggie/pkg/source/codec/json"
	"github.com/loggie-io/loggie/pkg/source/codec/regex"
	k8sMeta "github.com/loggie-io/loggie/pkg/util/pattern/k8smeta"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"regexp"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/source"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/listers/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/source/file"
	"github.com/loggie-io/loggie/pkg/util"
)

const (
	GenerateConfigName           = "kube-loggie.yml"
	GenerateTypeLoggieConfigName = "cluster-config.yml"
	GenerateTypeNodeConfigName   = "node-config.yml"
)

type KubeFileSourceExtra struct {
	ContainerName            string           `yaml:"containerName,omitempty"`
	MatchFields              *matchFields     `yaml:"matchFields,omitempty"`
	ExcludeContainerPatterns []string         `yaml:"excludeContainerPatterns,omitempty"` // regular pattern
	excludeContainerRegexps  []*regexp.Regexp `yaml:"-,omitempty"`
}

func GetKubeExtraFromFileSource(src *source.Config) (*KubeFileSourceExtra, error) {
	extra := &KubeFileSourceExtra{}

	if err := cfg.UnpackFromCommonCfg(src.Properties, extra).Do(); err != nil {
		return nil, err
	}
	return extra, nil
}

func (f *KubeFileSourceExtra) IsContainerExcluded(container string) bool {
	if len(f.excludeContainerRegexps) == 0 {
		return false
	}
	for _, excludeContainerRegexp := range f.excludeContainerRegexps {
		if excludeContainerRegexp != nil && excludeContainerRegexp.Match([]byte(container)) {
			return true
		}
	}
	return false
}

func setFileSource(s *source.Config, f *file.Config) error {
	filesrc, err := cfg.Pack(f)
	if err != nil {
		return err
	}

	s.Properties = filesrc
	return nil
}

func getFileSource(s *source.Config) (*file.Config, error) {
	conf := &file.Config{}
	if err := cfg.UnpackFromCommonCfg(s.Properties, conf).Do(); err != nil {
		return nil, errors.WithMessage(err, "unpack file source config failed")
	}
	return conf, nil
}

type matchFields struct {
	LabelKey      []string `yaml:"labelKey,omitempty"`
	AnnotationKey []string `yaml:"annotationKey,omitempty"`
	Env           []string `yaml:"env,omitempty"`
}

func (c *Controller) handleLogConfigTypePodAddOrUpdate(lgc *logconfigv1beta1.LogConfig) (err error, podsName []string) {
	// find pods related in the node
	podList, err := helper.GetLogConfigRelatedPod(lgc, c.podsLister)
	if err != nil {
		return err, nil
	}
	if len(podList) == 0 {
		log.Info("logConfig %s/%s matches no pods", lgc.Namespace, lgc.Name)
		return nil, nil
	}

	var successPodNames []string
	var errs []error
	for _, pod := range podList {
		if err := c.handleLogConfigPerPod(lgc, pod); err != nil {
			errs = append(errs, errors.WithMessagef(err, "match pod %s/%s", pod.Namespace, pod.Name))
			continue
		}
		successPodNames = append(successPodNames, pod.Name)
	}
	if len(errs) > 0 {
		return utilerrors.NewAggregate(errs), successPodNames
	}
	return nil, successPodNames
}

func (c *Controller) handlePodAddOrUpdate(pod *corev1.Pod) error {

	// check if pod is in the index
	if c.typePodIndex.IsPodExist(pod.Namespace, pod.Name) {
		log.Info("pod: %s/%s is in index, ignore pod addOrUpdate event", pod.Namespace, pod.Name)
		return nil
	}

	c.handlePodAddOrUpdateOfLogConfig(pod)

	c.handlePodAddOrUpdateOfClusterLogConfig(pod)

	return nil
}

func (c *Controller) handlePodAddOrUpdateOfLogConfig(pod *corev1.Pod) {
	// label selected logConfigs
	lgcList, err := helper.GetPodRelatedLogConfigs(pod, c.logConfigLister)
	if err != nil || len(lgcList) == 0 {
		return
	}

	for _, lgc := range lgcList {
		if !c.belongOfCluster(lgc.Spec.Selector.Cluster) {
			continue
		}

		if err := lgc.Validate(); err != nil {
			continue
		}

		if err := c.handleLogConfigPerPod(lgc, pod); err != nil {
			msg := fmt.Sprintf(MessageSyncFailed, lgc.Spec.Selector.Type, pod.Name, err.Error())
			c.record.Event(lgc, corev1.EventTypeWarning, ReasonFailed, msg)
			return
		}
		msg := fmt.Sprintf(MessageSyncSuccess, lgc.Spec.Selector.Type, pod.Name)
		c.record.Event(lgc, corev1.EventTypeNormal, ReasonSuccess, msg)
	}
}

func (c *Controller) handlePodAddOrUpdateOfClusterLogConfig(pod *corev1.Pod) {
	// label selected clusterLogConfigs
	clgcList, err := helper.GetPodRelatedClusterLogConfigs(pod, c.clusterLogConfigLister)
	if err != nil || len(clgcList) == 0 {
		return
	}

	for _, clgc := range clgcList {

		if !c.belongOfCluster(clgc.Spec.Selector.Cluster) {
			continue
		}

		if err := clgc.Validate(); err != nil {
			continue
		}

		if err := c.handleLogConfigPerPod(clgc.ToLogConfig(), pod); err != nil {
			msg := fmt.Sprintf(MessageSyncFailed, clgc.Spec.Selector.Type, pod.Name, err.Error())
			c.record.Event(clgc, corev1.EventTypeWarning, ReasonFailed, msg)
			return
		}
		msg := fmt.Sprintf(MessageSyncSuccess, clgc.Spec.Selector.Type, pod.Name)
		c.record.Event(clgc, corev1.EventTypeNormal, ReasonSuccess, msg)
	}
}

func (c *Controller) handleLogConfigPerPod(lgc *logconfigv1beta1.LogConfig, pod *corev1.Pod) error {

	// generate pod related pipeline configs
	pipeRaw, err := c.getConfigFromPodAndLogConfig(lgc, pod, c.sinkLister, c.interceptorLister)
	if err != nil {
		return err
	}
	if pipeRaw == nil {
		return nil
	}

	if err := cfg.NewUnpack(nil, pipeRaw, nil).Defaults().Validate().Do(); err != nil {
		return err
	}

	// get pod related pipeline configs from index
	cfgsInIndex := c.typePodIndex.GetPipeConfigs(pod.Namespace, pod.Name, lgc.Namespace, lgc.Name)

	// compare and check if we should update
	// FIXME Array order may causes inequality
	if cmp.Equal(pipeRaw, cfgsInIndex) {
		return nil
	}

	// update index
	if err = c.typePodIndex.ValidateAndSetConfigs(pod.Namespace, pod.Name, lgc.Name, pipeRaw, lgc); err != nil {
		return err
	}

	// TODO merge pipelines if there is no specific pipeline configs
	err = c.SyncConfigToFile(logconfigv1beta1.SelectorTypePod)
	if err != nil {
		return errors.WithMessage(err, "sync config to file failed")
	}
	// TODO update status when success
	if !c.config.AsyncFlush.Enabled {
		if lgc.Namespace == "" {
			log.Info("handle clusterLogConfig %s addOrUpdate event and sync config file success, related pod: %s", lgc.Name, pod.Name)
		} else {
			log.Info("handle logConfig %s/%s addOrUpdate event and sync config file success, related pod: %s", lgc.Namespace, lgc.Name, pod.Name)
		}
	}

	return nil
}

func (c *Controller) getConfigFromPodAndLogConfig(lgc *logconfigv1beta1.LogConfig, pod *corev1.Pod,
	sinkLister v1beta1.SinkLister, interceptorLister v1beta1.InterceptorLister) (*pipeline.Config, error) {

	if len(pod.Status.ContainerStatuses) == 0 {
		return nil, nil
	}
	cfgs, err := c.getConfigFromContainerAndLogConfig(lgc, pod, sinkLister, interceptorLister)
	if err != nil {
		return nil, err
	}
	return cfgs, nil
}

func (c *Controller) getConfigFromContainerAndLogConfig(lgc *logconfigv1beta1.LogConfig, pod *corev1.Pod,
	sinkLister v1beta1.SinkLister, interceptorLister v1beta1.InterceptorLister) (*pipeline.Config, error) {

	logConf := lgc.DeepCopy()
	sourceConfList := make([]*source.Config, 0)
	err := cfg.UnPackFromRaw([]byte(logConf.Spec.Pipeline.Sources), &sourceConfList).Do()
	if err != nil {
		return nil, errors.WithMessagef(err, "unpack logConfig %s sources failed", lgc.Namespace)
	}

	filesources, err := c.updateSources(sourceConfList, pod, logConf.Name)
	if err != nil {
		return nil, err
	}
	pipecfg, err := toPipeConfig(lgc.Namespace, lgc.Name, logConf.Spec.Pipeline, filesources, sinkLister, interceptorLister)
	if err != nil {
		return nil, err
	}
	return pipecfg, nil
}

func (c *Controller) updateSources(sourceConfList []*source.Config, pod *corev1.Pod, logConfigName string) ([]*source.Config, error) {
	filesources := make([]*source.Config, 0)
	for _, sourceConf := range sourceConfList {
		filesrc, err := c.makeConfigPerSource(sourceConf, pod, logConfigName)
		if err != nil {
			return nil, err
		}
		filesources = append(filesources, filesrc...)
	}
	return filesources, nil
}

func (c *Controller) makeConfigPerSource(s *source.Config, pod *corev1.Pod, logconfigName string) ([]*source.Config, error) {
	extra, err := GetKubeExtraFromFileSource(s)
	if err != nil {
		return nil, err
	}
	if len(extra.ExcludeContainerPatterns) > 0 {
		regexps := make([]*regexp.Regexp, len(extra.ExcludeContainerPatterns))
		for i, containerPattern := range extra.ExcludeContainerPatterns {
			reg, err := regexp.Compile(containerPattern)
			if err != nil {
				log.Error("compile exclude container pattern(%s) fail: %v", containerPattern, err)
				continue
			}
			regexps[i] = reg
		}
		extra.excludeContainerRegexps = regexps
	}

	filesrcList := make([]*source.Config, 0)
	for _, status := range pod.Status.ContainerStatuses {
		filesrc := &source.Config{}
		if err := util.Clone(s, &filesrc); err != nil {
			return nil, err
		}

		containerId := helper.ExtractContainerId(status.ContainerID)
		if filesrc.Type != file.Type {
			return nil, errors.New("only source type=file is supported when selector.type=pod")
		}

		if extra.IsContainerExcluded(status.Name) {
			continue
		}

		if extra.ContainerName != "" && extra.ContainerName != status.Name {
			continue
		}
		// change the source name, add pod.Name-containerName as prefix, since there maybe multiple containers in pod
		filesrc.Name = genTypePodSourceName(pod.Name, status.Name, filesrc.Name)

		// inject default pod metadata
		if err := c.injectFields(filesrc, c.config, extra.MatchFields, pod, logconfigName, status.Name); err != nil {
			return nil, err
		}

		// use paths of the node
		if err := c.updatePaths(filesrc, c.config, pod, status.Name, containerId); err != nil {
			return nil, err
		}

		filesrcList = append(filesrcList, filesrc)
	}

	return filesrcList, nil
}

func genTypePodSourceName(podName string, containerName string, sourceName string) string {
	return fmt.Sprintf("%s/%s/%s", podName, containerName, sourceName)
}

func getTypePodOriginSourceName(podSourceName string) string {
	res := strings.Split(podSourceName, "/")
	if len(res) == 0 {
		return ""
	}
	return res[len(res)-1]
}

func (c *Controller) updatePaths(source *source.Config, config *Config, pod *corev1.Pod, containerName, containerId string) error {
	filecfg, err := getFileSource(source)
	if err != nil {
		return err
	}
	// update paths with real paths in the node
	nodePaths, err := c.getPathsInNode(config, filecfg.CollectConfig.Paths, pod, containerName, containerId)
	if err != nil {
		return err
	}

	// parse the stdout raw logs
	if config.ParseStdout && len(filecfg.CollectConfig.Paths) == 1 && filecfg.CollectConfig.Paths[0] == logconfigv1beta1.PathStdout {
		source.Codec = &codec.Config{}
		if config.ContainerRuntime == runtime.RuntimeDocker {
			// e.g.: `{"log":"example: 17 Tue Feb 16 09:15:17 UTC 2021\n","stream":"stdout","time":"2021-02-16T09:15:17.511829776Z"}`
			source.Codec.Type = json.Type
			jsoncodec := json.Config{
				BodyFields: "log",
			}
			if source.Codec.CommonCfg, err = cfg.Pack(&jsoncodec); err != nil {
				return errors.WithMessage(err, "pack json codec config failed")
			}

		} else {
			// e.g.: `2021-02-16T09:21:20.545525544Z stdout F example: 15 Tue Feb 16 09:21:20 UTC 2021`
			source.Codec.Type = regex.Type
			regexcodec := regex.Config{
				Pattern:    "^(?P<time>[^ ^Z]+Z) (?P<stream>stdout|stderr) (?P<logtag>[^ ]*) (?P<log>.*)$",
				BodyFields: "log",
			}
			if source.Codec.CommonCfg, err = cfg.Pack(&regexcodec); err != nil {
				return errors.WithMessage(err, "pack regex codec config failed")
			}
		}
	}

	filecfg.CollectConfig.Paths = nodePaths
	err = setFileSource(source, filecfg)
	if err != nil {
		return err
	}
	return nil
}

func (c *Controller) getPathsInNode(config *Config, containerPaths []string, pod *corev1.Pod, containerName string, containerId string) ([]string, error) {
	if len(containerPaths) == 0 {
		return nil, errors.New("path is empty")
	}

	return helper.PathsInNode(config.PodLogDirPrefix, config.KubeletRootDir, config.RootFsCollectionEnabled, c.runtime, containerPaths, pod, containerId, containerName)
}

func (c *Controller) injectFields(src *source.Config, config *Config, match *matchFields, pod *corev1.Pod, lgcName string, containerName string) error {
	if src.Fields == nil {
		src.Fields = make(map[string]interface{})
	}

	m := config.Fields
	if m.Namespace != "" {
		src.Fields[m.Namespace] = pod.Namespace
	}
	if m.NodeName != "" {
		src.Fields[m.NodeName] = pod.Spec.NodeName
	}
	if m.NodeIP != "" {
		src.Fields[m.NodeIP] = pod.Status.HostIP
	}
	if m.PodName != "" {
		src.Fields[m.PodName] = pod.Name
	}
	if m.PodIP != "" {
		src.Fields[m.PodIP] = pod.Status.PodIP
	}
	if m.ContainerName != "" {
		src.Fields[m.ContainerName] = containerName
	}
	if m.LogConfig != "" {
		src.Fields[m.LogConfig] = lgcName
	}

	if len(c.extraFieldsPattern) > 0 {
		for k, p := range c.extraFieldsPattern {
			res, err := p.WithK8s(k8sMeta.NewFieldsData(pod, containerName, lgcName)).Render()
			if err != nil {
				log.Warn("add extra k8s fields %s failed: %v", k, err)
				continue
			}
			src.Fields[k] = res
		}
	}

	if match != nil {
		if len(match.LabelKey) > 0 {
			for k, v := range helper.GetMatchedPodLabel(match.LabelKey, pod) {
				src.Fields[k] = v
			}
		}
		if len(match.AnnotationKey) > 0 {
			for k, v := range helper.GetMatchedPodAnnotation(match.AnnotationKey, pod) {
				src.Fields[k] = v
			}
		}
		if len(match.Env) > 0 {
			for k, v := range helper.GetMatchedPodEnv(match.Env, pod, containerName) {
				src.Fields[k] = v
			}
		}
	}

	return nil
}

func toPipeConfig(lgcNamespace string, lgcName string, lgcPipe *logconfigv1beta1.Pipeline, filesources []*source.Config, sinkLister v1beta1.SinkLister, interceptorLister v1beta1.InterceptorLister) (*pipeline.Config, error) {
	pipecfg := &pipeline.Config{}

	if lgcNamespace == "" {
		pipecfg.Name = lgcName
	} else {
		pipecfg.Name = fmt.Sprintf("%s/%s", lgcNamespace, lgcName)
	}

	pipecfg.Sources = filesources

	sink, err := helper.ToPipelineSink(lgcPipe.Sink, lgcPipe.SinkRef, sinkLister)
	if err != nil {
		return pipecfg, err
	}
	pipecfg.Sink = sink

	interceptors, err := toPipelineInterceptorWithPodInject(lgcPipe.Interceptors, lgcPipe.InterceptorRef, interceptorLister, filesources)
	if err != nil {
		return pipecfg, err
	}
	pipecfg.Interceptors = interceptors

	return pipecfg, nil
}

// Since the source name is auto-generated in LogConfig, the interceptor param `belongTo` is also need to be changed
func toPipelineInterceptorWithPodInject(interceptorRaw string, interceptorRef string, interceptorLister v1beta1.InterceptorLister, filesources []*source.Config) ([]*interceptor.Config, error) {
	var interceptorStr string
	if interceptorRaw != "" {
		interceptorStr = interceptorRaw
	} else {
		lgcInterceptor, err := interceptorLister.Get(interceptorRef)
		if err != nil {
			if kerrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}

		interceptorStr = lgcInterceptor.Spec.Interceptors
	}

	// key: originSourceName, multi value: podName/containerName/originSourceName
	originSrcNameMap := make(map[string]sets.String)
	for _, fs := range filesources {
		podSrcName := fs.Name
		origin := getTypePodOriginSourceName(podSrcName)
		srcVal, ok := originSrcNameMap[origin]
		if !ok {
			originSrcNameMap[origin] = sets.NewString(podSrcName)
			continue
		}
		srcVal.Insert(podSrcName)
	}

	icpConfList := make([]*interceptor.Config, 0)
	err := cfg.UnPackFromRaw([]byte(interceptorStr), &icpConfList).Do()
	if err != nil {
		return nil, err
	}

	for i, extIcp := range icpConfList {
		exd, err := extIcp.GetExtension()
		if err != nil {
			return nil, err
		}
		if len(exd.BelongTo) == 0 {
			continue
		}

		newBelongTo := make([]string, 0)
		// update interceptor belongTo with podName/containerName/originSourceName
		for _, origin := range exd.BelongTo {
			podSrcNameSet, ok := originSrcNameMap[origin]
			if !ok {
				continue
			}
			newBelongTo = append(newBelongTo, podSrcNameSet.List()...)
		}

		icpConfList[i].SetBelongTo(newBelongTo)
	}

	return icpConfList, nil
}
