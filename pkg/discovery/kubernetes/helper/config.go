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

package helper

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/control"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/core/sink"
	"github.com/loggie-io/loggie/pkg/core/source"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/listers/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/pipeline"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"strings"
)

func ToPipeline(lgc *logconfigv1beta1.LogConfig, sinkLister v1beta1.SinkLister, interceptorLister v1beta1.InterceptorLister) (*control.PipelineConfig, error) {
	pipelineCfg := &control.PipelineConfig{}
	var pipRaws []pipeline.Config
	pip := lgc.Spec.Pipeline

	pipRaw := pipeline.Config{}
	pipRaw.Name = lgc.Name

	src, err := ToPipelineSources(pip.Sources)
	if err != nil {
		return nil, err
	}
	pipRaw.Sources = src

	inter, err := ToPipelineInterceptor(lgc.Spec.Pipeline.Interceptors, pip.InterceptorRef, interceptorLister)
	if err != nil {
		return nil, err
	}
	pipRaw.Interceptors = inter

	sk, err := ToPipelineSink(lgc.Spec.Pipeline.Sink, pip.SinkRef, sinkLister)
	if err != nil {
		return nil, err
	}
	pipRaw.Sink = sk

	pipRaws = append(pipRaws, pipRaw)

	pipelineCfg.SetPipelines(pipRaws)
	return pipelineCfg, nil
}

func ToPipelineSources(sources string) ([]*source.Config, error) {
	sourceCfg := make([]*source.Config, 0)
	err := cfg.UnPackFromRaw([]byte(sources), &sourceCfg).Do()
	if err != nil {
		return nil, err
	}
	return sourceCfg, nil
}

func ToPipelineSink(sinkRaw string, sinkRef string, sinkLister v1beta1.SinkLister) (*sink.Config, error) {
	// we use the sink in logConfig other than sinkRef if sink content is not empty
	var sinkStr string
	if sinkRaw != "" {
		sinkStr = sinkRaw
	} else {
		lgcSink, err := sinkLister.Get(sinkRef)
		if err != nil {
			if kerrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}

		sinkStr = lgcSink.Spec.Sink
	}

	sinkConf := sink.Config{}
	err := cfg.UnPackFromRaw([]byte(sinkStr), &sinkConf).Do()
	if err != nil {
		return nil, err
	}

	return &sinkConf, nil
}

func ToPipelineInterceptor(interceptorsRaw string, interceptorRef string, interceptorLister v1beta1.InterceptorLister) ([]*interceptor.Config, error) {
	var icp string
	if interceptorsRaw != "" {
		icp = interceptorsRaw
	} else {
		lgcInterceptor, err := interceptorLister.Get(interceptorRef)
		if err != nil {
			if kerrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}

		icp = lgcInterceptor.Spec.Interceptors
	}

	interConfList := make([]*interceptor.Config, 0)
	err := cfg.UnPackFromRaw([]byte(icp), &interConfList).Do()
	if err != nil {
		return nil, err
	}

	return interConfList, nil
}

func GenTypePodSourceName(lgcNamespace string, podNamespace string, podName string, containerName string, sourceName string) string {
	// if lgcNamespace is empty, we use podNamespace as the first part of the source name,
	// because this is the pod matched by clusterLogConfig, if the pod namespace is not added, it may cause the source to be duplicated
	if lgcNamespace == "" {
		return fmt.Sprintf("%s/%s/%s/%s", podNamespace, podName, containerName, sourceName)
	}
	return fmt.Sprintf("%s/%s/%s", podName, containerName, sourceName)
}

func GetTypePodOriginSourceName(podSourceName string) string {
	res := strings.Split(podSourceName, "/")
	if len(res) == 0 {
		return ""
	}
	return res[len(res)-1]
}

func GetPathsFromSources(src []*source.Config) []string {
	var paths []string
	for _, s := range src {
		p := GetPathsFromSource(s)
		paths = append(paths, p...)
	}

	return paths
}

func GetPathsFromSource(src *source.Config) []string {
	pathsRaw := src.Properties["paths"]
	paths, ok := pathsRaw.([]string)
	if !ok {
		ipaths := pathsRaw.([]interface{})
		// type convert to []string
		for _, v := range ipaths {
			p := v.(string)
			paths = append(paths, p)
		}
	}

	return paths
}

func SetPathsToSource(src *source.Config, paths []string) {
	src.Properties["paths"] = paths
}
