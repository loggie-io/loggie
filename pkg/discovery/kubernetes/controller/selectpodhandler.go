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
	"path/filepath"
	"regexp"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/queue"
	"github.com/loggie-io/loggie/pkg/core/source"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/listers/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/index"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/runtime"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/source/codec"
	"github.com/loggie-io/loggie/pkg/source/codec/json"
	"github.com/loggie-io/loggie/pkg/source/codec/regex"
	"github.com/loggie-io/loggie/pkg/source/file"
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/loggie-io/loggie/pkg/util/pattern"
)

const (
	GenerateConfigName           = "kube-loggie.yml"
	GenerateTypeLoggieConfigName = "cluster-config.yml"
	GenerateTypeNodeConfigName   = "node-config.yml"
	GenerateTypeVmConfigName     = "vm-config.yml"
)

type KubeFileSourceExtra struct {
	ContainerName            string           `yaml:"containerName,omitempty"`
	MatchFields              *matchFields     `yaml:"matchFields,omitempty"`
	TypePodFields            KubeMetaFields   `yaml:"typePodFields,omitempty"`
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

	// Reformat the key of label/annotation/env in the pod, the value remains unchanged
	// Example: According to the regular, extract values, and then add a prefix
	// Assumption: pod label is `aa.bb/foo=bar`
	// Configure:
	// ```
	// reformatKeys:
	//   label:
	//   - regex: aa.bb/(.*)
	//     replace: pre-${1}
	// ```
	// Result: the field added to the log is `pre-foo=bar`
	FmtKey *fieldsFmt `yaml:"reformatKeys,omitempty"`
}

type fieldsFmt struct {
	Label      []fmtKey `yaml:"label,omitempty"`
	Annotation []fmtKey `yaml:"annotation,omitempty"`
	Env        []fmtKey `yaml:"env,omitempty"`
}

type fmtKey struct {
	Regex   string `yaml:"regex,omitempty"`
	Replace string `yaml:"replace,omitempty"`
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
		if !helper.IsPodReady(pod) {
			continue
		}

		if err := c.handleLogConfigPerPod(lgc, pod); err != nil {
			errs = append(errs, errors.WithMessagef(err, "pod %s/%s", pod.Namespace, pod.Name))
			continue
		}
		successPodNames = append(successPodNames, pod.Name)
	}
	if len(errs) > 0 {
		// To prevent missing long reports, only a part of the error can be shown here
		if len(errs) > 2 {
			errs = errs[:2]
			errs = append(errs, errors.New("..."))
		}
		return utilerrors.NewAggregate(errs), successPodNames
	}
	return nil, successPodNames
}

func (c *Controller) handlePodAddOrUpdate(pod *corev1.Pod) error {

	// check if pod is in the index or container id has changed
	if !c.typePodIndex.IsPodUpdated(pod) {
		log.Info("pod: %s/%s is in index and unchanged, ignore pod addOrUpdate event", pod.Namespace, pod.Name)
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
		if !c.belongOfCluster(lgc.Spec.Selector.Cluster, lgc.Annotations) {
			continue
		}

		if err := lgc.Validate(); err != nil {
			continue
		}

		if err := c.handleLogConfigPerPod(lgc, pod); err != nil {
			log.Warn("sync %s %v failed: %s", lgc.Spec.Selector.Type, pod.Name, err.Error())
			return
		}
		log.Info("handle pod %s/%s addOrUpdate event and sync config file success, related logConfig is %s", pod.Namespace, pod.Name, lgc.Name)
		c.record.Eventf(lgc, corev1.EventTypeNormal, ReasonSuccess, MessageSyncSuccess, lgc.Spec.Selector.Type, pod.Name)
	}
}

func (c *Controller) handlePodAddOrUpdateOfClusterLogConfig(pod *corev1.Pod) {
	// label selected clusterLogConfigs
	clgcList, err := helper.GetPodRelatedClusterLogConfigs(pod, c.clusterLogConfigLister)
	if err != nil || len(clgcList) == 0 {
		return
	}

	for _, clgc := range clgcList {
		if !c.belongOfCluster(clgc.Spec.Selector.Cluster, clgc.Annotations) {
			continue
		}

		if err := clgc.Validate(); err != nil {
			continue
		}

		if err := c.handleLogConfigPerPod(clgc.ToLogConfig(), pod); err != nil {
			log.Warn("sync %s %v failed: %s", clgc.Spec.Selector.Type, pod.Name, err.Error())
			return
		}
		log.Info("handle pod %s/%s addOrUpdate event and sync config file success, related clusterLogConfig is %s", pod.Namespace, pod.Name, clgc.Name)
		c.record.Eventf(clgc, corev1.EventTypeNormal, ReasonSuccess, MessageSyncSuccess, clgc.Spec.Selector.Type, pod.Name)
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

	pipeRawCopy := pipeRaw.DeepCopy()
	if err := cfg.NewUnpack(nil, pipeRawCopy, nil).Defaults().Validate().Do(); err != nil {
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
	c.typePodIndex.SetConfigs(pod, lgc.Name, pipeRaw, lgc)
	if c.config.DynamicContainerLog {
		paths := helper.GetPathsFromSources(pipeRaw.Sources)
		log.Info("[pipeline: %s] [pod: %s/%s] set dynamic paths: %+v", pipeRaw.Name, pod.Namespace, pod.Name, paths)
	}

	// TODO merge pipelines if there is no specific pipeline configs
	err = c.syncConfigToFile(logconfigv1beta1.SelectorTypePod)
	if err != nil {
		return errors.WithMessage(err, "sync config to file failed")
	}
	// TODO update status when success
	if lgc.Namespace == "" {
		log.Info("handle clusterLogConfig %s addOrUpdate event and sync config file success, related pod: %s/%s", lgc.Name, pod.Namespace, pod.Name)
	} else {
		log.Info("handle logConfig %s/%s addOrUpdate event and sync config file success, related pod: %s", lgc.Namespace, lgc.Name, pod.Name)
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
	pipecfg, err := toPipeConfig(c.config.DynamicContainerLog, lgc.Namespace, lgc.Name, logConf.Spec.Pipeline, filesources, sinkLister, interceptorLister)
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
		filesrc.Name = helper.GenTypePodSourceName(pod.Name, status.Name, filesrc.Name)

		// inject default pod metadata
		if err := c.injectTypePodFields(c.config.DynamicContainerLog, filesrc, extra, pod, logconfigName, status.Name); err != nil {
			return nil, err
		}

		// use paths of the node
		if err := c.updatePaths(filesrc, pod, status.Name, containerId); err != nil {
			return nil, err
		}

		filesrcList = append(filesrcList, filesrc)
	}

	return filesrcList, nil
}

func (c *Controller) updatePaths(source *source.Config, pod *corev1.Pod, containerName, containerId string) error {
	filecfg, err := getFileSource(source)
	if err != nil {
		return err
	}
	// update paths with real paths in the node
	nodePaths, err := c.getPathsInNode(filecfg.CollectConfig.Paths, pod, containerName, containerId)
	if err != nil {
		return err
	}

	// parse the stdout raw logs
	if c.config.ParseStdout && len(filecfg.CollectConfig.Paths) == 1 && filecfg.CollectConfig.Paths[0] == logconfigv1beta1.PathStdout {
		source.Codec = &codec.Config{}
		if c.config.ContainerRuntime == runtime.RuntimeDocker {
			// e.g.: `{"log":"example: 17 Tue Feb 16 09:15:17 UTC 2021\n","stream":"stdout","time":"2021-02-16T09:15:17.511829776Z"}`
			source.Codec.Type = json.Type
			jsoncodec := json.Config{
				BodyFields: "log",
			}
			if source.Codec.CommonCfg, err = cfg.Pack(&jsoncodec); err != nil {
				return errors.WithMessage(err, "pack json codec config failed")
			}

		} else if c.config.ContainerRuntime == runtime.RuntimeContainerd {
			// e.g.: `2021-02-16T09:21:20.545525544Z stdout F example: 15 Tue Feb 16 09:21:20 UTC 2021`
			source.Codec.Type = regex.Type
			regexcodec := regex.Config{
				Pattern:    `^(?P<time>\S+) (?P<stream>stdout|stderr) (?P<logtag>[^ ]*) (?P<log>.*)$`,
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

func (c *Controller) getPathsInNode(containerPaths []string, pod *corev1.Pod, containerName string, containerId string) ([]string, error) {
	if len(containerPaths) == 0 {
		return nil, errors.New("path is empty")
	}

	paths, err := helper.PathsInNode(c.config.PodLogDirPrefix, c.config.KubeletRootDir, c.config.RootFsCollectionEnabled, c.runtime, containerPaths, pod, containerId, containerName)
	if err != nil || len(c.config.HostRootMountPath) == 0 {
		return paths, err
	}

	newPaths := make([]string, 0, len(paths))
	rootPath := c.config.HostRootMountPath
	for _, path := range paths {
		newPaths = append(newPaths, filepath.Join(rootPath, path))
	}

	return newPaths, err
}

func (c *Controller) injectTypePodFields(dynamicContainerLogs bool, src *source.Config, extra *KubeFileSourceExtra, pod *corev1.Pod, lgcName string, containerName string) error {
	if src.Fields == nil {
		src.Fields = make(map[string]interface{})
	}

	k8sFields := make(map[string]interface{})

	// Deprecated
	m := c.config.Fields
	if m.Namespace != "" {
		k8sFields[m.Namespace] = pod.Namespace
	}
	if m.NodeName != "" {
		k8sFields[m.NodeName] = pod.Spec.NodeName
	}
	if m.NodeIP != "" {
		k8sFields[m.NodeIP] = pod.Status.HostIP
	}
	if m.PodName != "" {
		k8sFields[m.PodName] = pod.Name
	}
	if m.PodIP != "" {
		k8sFields[m.PodIP] = pod.Status.PodIP
	}
	if m.ContainerName != "" {
		k8sFields[m.ContainerName] = containerName
	}
	if m.LogConfig != "" {
		k8sFields[m.LogConfig] = lgcName
	}

	if len(c.extraTypePodFieldsPattern) > 0 {
		for k, v := range renderTypePodFieldsPattern(c.extraTypePodFieldsPattern, pod, containerName, lgcName) {
			k8sFields[k] = v
		}
	}

	podFields := extra.TypePodFields
	if len(podFields) > 0 {
		if err := podFields.validate(); err != nil {
			return err
		}
		podPattern := podFields.initPattern()
		for k, v := range renderTypePodFieldsPattern(podPattern, pod, containerName, lgcName) {
			k8sFields[k] = v
		}
	}

	match := extra.MatchFields
	if match != nil {
		if len(match.LabelKey) > 0 {
			for k, v := range helper.GetMatchedPodLabel(match.LabelKey, pod) {
				k8sFields[k] = v
			}
		}
		if len(match.AnnotationKey) > 0 {
			for k, v := range helper.GetMatchedPodAnnotation(match.AnnotationKey, pod) {
				k8sFields[k] = v
			}
		}
		if len(match.Env) > 0 {
			for k, v := range helper.GetMatchedPodEnv(match.Env, pod, containerName) {
				k8sFields[k] = v
			}
		}

		if match.FmtKey != nil {
			ret, err := fmtFieldsKey(pod, containerName, match.FmtKey)
			if err != nil {
				return err
			}
			for k, v := range ret {
				k8sFields[k] = v
			}
		}
	}

	// The k8s meta information here will not be rendered as configuration and will eventually be set to external.DynamicLogIndexer
	if dynamicContainerLogs {
		src.Fields[index.K8sFieldsKey] = k8sFields
		return nil
	}

	for k, v := range k8sFields {
		src.Fields[k] = v
	}

	return nil
}

func fmtFieldsKey(pod *corev1.Pod, containerName string, fmt *fieldsFmt) (map[string]string, error) {
	result := make(map[string]string)
	if fmt == nil {
		return result, nil
	}

	// reformat pod label keys
	if len(fmt.Label) > 0 {
		for _, fmtLabel := range fmt.Label {
			if fmtLabel.Regex == "" || fmtLabel.Replace == "" {
				continue
			}

			ret, err := regexAndReplace(pod.Labels, fmtLabel.Regex, fmtLabel.Replace)
			if err != nil {
				return result, err
			}
			for k, v := range ret {
				result[k] = v
			}
		}
	}

	// reformat pod annotation keys
	if len(fmt.Annotation) > 0 {
		for _, fmtAnno := range fmt.Annotation {
			if fmtAnno.Regex == "" || fmtAnno.Replace == "" {
				continue
			}

			ret, err := regexAndReplace(pod.Annotations, fmtAnno.Regex, fmtAnno.Replace)
			if err != nil {
				return result, err
			}
			for k, v := range ret {
				result[k] = v
			}
		}
	}

	// reformat pod env keys
	if len(fmt.Env) > 0 {
		for _, fmtEnv := range fmt.Env {
			if fmtEnv.Regex == "" || fmtEnv.Replace == "" {
				continue
			}

			envs := helper.GetMatchedPodEnv([]string{"*"}, pod, containerName)
			ret, err := regexAndReplace(envs, fmtEnv.Regex, fmtEnv.Replace)
			if err != nil {
				return result, err
			}
			for k, v := range ret {
				result[k] = v
			}
		}
	}

	return result, nil
}

func regexAndReplace(in map[string]string, regex string, replace string) (map[string]string, error) {
	result := make(map[string]string)
	p, err := util.CompilePatternWithJavaStyle(regex)
	if err != nil {
		return nil, err
	}

	for k, v := range in {
		matched := p.FindStringSubmatch(k)
		if len(matched) > 0 {
			fmtK := p.ReplaceAllString(k, replace)
			result[fmtK] = v
		}
	}

	return result, nil
}

func toPipeConfig(dynamicContainerLog bool, lgcNamespace string, lgcName string, lgcPipe *logconfigv1beta1.Pipeline, filesources []*source.Config, sinkLister v1beta1.SinkLister, interceptorLister v1beta1.InterceptorLister) (*pipeline.Config, error) {
	pipecfg := &pipeline.Config{}

	pipecfg.Name = helper.MetaNamespaceKey(lgcNamespace, lgcName)
	pipecfg.Sources = filesources

	queueConf, err := toPipelineQueue(lgcPipe.Queue)
	if err != nil {
		return pipecfg, err
	}
	pipecfg.Queue = queueConf

	sink, err := helper.ToPipelineSink(lgcPipe.Sink, lgcPipe.SinkRef, sinkLister)
	if err != nil {
		return pipecfg, err
	}
	pipecfg.Sink = sink

	interceptors, err := toPipelineInterceptorWithPodInject(dynamicContainerLog, lgcPipe.Interceptors, lgcPipe.InterceptorRef, interceptorLister, filesources)
	if err != nil {
		return pipecfg, err
	}
	pipecfg.Interceptors = interceptors

	return pipecfg, nil
}

// Since the source name is auto-generated in LogConfig, the interceptor param `belongTo` is also need to be changed
func toPipelineInterceptorWithPodInject(dynamicContainerLog bool, interceptorRaw string, interceptorRef string, interceptorLister v1beta1.InterceptorLister, filesources []*source.Config) ([]*interceptor.Config, error) {
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

	icpConfList := make([]*interceptor.Config, 0)
	err := cfg.UnPackFromRaw([]byte(interceptorStr), &icpConfList).Do()
	if err != nil {
		return nil, err
	}

	if dynamicContainerLog {
		return icpConfList, nil
	}

	// key: originSourceName, multi value: podName/containerName/originSourceName
	originSrcNameMap := make(map[string]sets.String)
	for _, fs := range filesources {
		podSrcName := fs.Name
		origin := helper.GetTypePodOriginSourceName(podSrcName)
		srcVal, ok := originSrcNameMap[origin]
		if !ok {
			originSrcNameMap[origin] = sets.NewString(podSrcName)
			continue
		}
		srcVal.Insert(podSrcName)
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

func renderTypePodFieldsPattern(pm map[string]*pattern.Pattern, pod *corev1.Pod, containerName string, logConfig string) map[string]interface{} {
	fields := make(map[string]interface{}, len(pm))
	for k, p := range pm {
		res, err := p.WithK8sPod(pattern.NewTypePodFieldsData(pod, containerName, logConfig)).Render()
		if err != nil {
			log.Warn("add extra k8s fields %s failed: %v", k, err)
			continue
		}
		fields[k] = res
	}
	return fields
}

func toPipelineQueue(queueRaw string) (*queue.Config, error) {
	if len(queueRaw) == 0 {
		return nil, nil
	}

	queueConf := queue.Config{}
	err := cfg.UnPackFromRaw([]byte(queueRaw), &queueConf).Do()
	if err != nil {
		return nil, err
	}

	return &queueConf, nil
}
