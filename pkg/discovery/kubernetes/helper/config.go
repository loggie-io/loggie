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
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/listers/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/pipeline"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
)

func ToPipeline(lgc *logconfigv1beta1.LogConfig, sinkLister v1beta1.SinkLister, interceptorLister v1beta1.InterceptorLister) (*control.PipelineRawConfig, error) {

	pipelineRawCfg := &control.PipelineRawConfig{}
	var pipRaws []pipeline.ConfigRaw
	pip := lgc.Spec.Pipeline

	pipRaw := pipeline.ConfigRaw{}
	pipRaw.Name = fmt.Sprintf("%s/%s/%s", lgc.Namespace, lgc.Name, pip.Name)

	src, err := ToPipelineSources(pip.Sources)
	if err != nil {
		return nil, err
	}
	pipRaw.Sources = src

	inter, err := ToPipelineInterceptor(pip.InterceptorRef, interceptorLister)
	if err != nil {
		return nil, err
	}
	pipRaw.Interceptors = inter

	sink, err := ToPipelineSink(pip.SinkRef, sinkLister)
	if err != nil {
		return nil, err
	}
	pipRaw.Sink = sink

	pipRaws = append(pipRaws, pipRaw)

	pipelineRawCfg.Pipelines = pipRaws
	return pipelineRawCfg, nil
}

func ToPipelineSources(sources string) ([]cfg.CommonCfg, error) {
	sourceCfg := make([]cfg.CommonCfg, 0)
	err := cfg.UnpackRaw([]byte(sources), &sourceCfg)
	if err != nil {
		return nil, err
	}

	return sourceCfg, nil
}

func ToPipelineSink(sinkRef string, sinkLister v1beta1.SinkLister) (cfg.CommonCfg, error) {
	lgcSink, err := sinkLister.Get(sinkRef)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	sinkConfList := cfg.NewCommonCfg()
	err = cfg.UnpackRaw([]byte(lgcSink.Spec.Sink), &sinkConfList)
	if err != nil {
		return nil, err
	}

	return sinkConfList, nil
}

func ToPipelineInterceptor(interceptorRef string, interceptorLister v1beta1.InterceptorLister) ([]cfg.CommonCfg, error) {
	lgcInterceptor, err := interceptorLister.Get(interceptorRef)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	interConfList := make([]cfg.CommonCfg, 0)
	err = cfg.UnpackRaw([]byte(lgcInterceptor.Spec.Interceptors), &interConfList)
	if err != nil {
		return nil, err
	}

	return interConfList, nil
}
