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

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/log"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/index"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

const (
	envSource      = "SOURCE"
	envSink        = "SINK"
	envInterceptor = "INTERCEPTOR"
)

type envLogConfig struct {
	name        string
	source      string
	sink        string
	interceptor string
}

func (c *Controller) handlePodEnvConfigs(pod *corev1.Pod) error {
	if c.typePodIndex.IsPodExist(pod.Namespace, pod.Name) {
		log.Info("pod: %s/%s is in index, ignore", pod.Namespace, pod.Name)
		return nil
	}

	if err := c.handleEnvsPerPod(pod); err != nil {
		log.Info("failed to handle pod %s/%s and sync config file",
			pod.Namespace, pod.Name)
		return err
	}

	return nil
}

func (c *Controller) handleEnvsPerPod(pod *corev1.Pod) error {
	configs := getLogConfigFromContainer(pod)
	for _, config := range configs {
		err := c.handleLogConfigPerContainer(pod, config)
		if err != nil {
			log.Warn("failed to handle container log config %v with error: %s", config, err.Error())
			continue
		}
	}

	err := c.syncConfigToFile(logconfigv1beta1.SelectorTypePod)
	if err != nil {
		return errors.WithMessage(err, "failed to sync config to files")
	}

	return nil
}

func (c *Controller) handleLogConfigPerContainer(pod *corev1.Pod, envConfig envLogConfig) error {
	pipeRaw, err := getPipelineConfig(c.config, pod, envConfig)
	if err != nil {
		return err
	}

	if pipeRaw == nil {
		return nil
	}

	pipeCopy, err := pipeRaw.DeepCopy()
	if err != nil {
		return errors.WithMessage(err, "deep copy pipeline envConfig error")
	}

	pipeCopy.SetDefaults()
	if err := pipeCopy.Validate(); err != nil {
		return err
	}

	if err := c.typePodIndex.ValidateAndSetConfigs(pod.Namespace, pod.Name, pod.Namespace, envConfig.name, pipeRaw); err != nil {
		return err
	}
	return nil
}

func getPipelineConfig(config *Config, pod *corev1.Pod, envConfig envLogConfig) (*pipeline.ConfigRaw, error) {
	logConfigName := fmt.Sprintf("%s/%s/%s", pod.Namespace, pod.Name, envConfig.name)
	sources := make([]fileSource, 0)
	err := cfg.UnpackRaw([]byte(envConfig.source), &sources)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to unpack source %s with error: %s", envConfig.source, err.Error())
	}

	updatedSources, err := updateSources(sources, config, pod, logConfigName)
	if err != nil {
		return nil, err
	}

	src, err := toPipelineSource(updatedSources)
	if err != nil {
		return nil, err
	}

	pipelineCfg := &pipeline.ConfigRaw{
		Name: logConfigName,
	}
	pipelineCfg.Sources = src

	sink := cfg.NewCommonCfg()
	err = cfg.UnpackRaw([]byte(envConfig.sink), &sink)
	if err != nil {
		return nil, err
	}
	pipelineCfg.Sink = sink

	interceptor, err := toPipelineInterceptors(updatedSources, envConfig.interceptor)
	if err != nil {
		return nil, err
	}
	pipelineCfg.Interceptors = interceptor

	return pipelineCfg, nil
}

func toPipelineInterceptors(sources []fileSource, interceptor string) ([]cfg.CommonCfg, error) {
	sourceNames := make(map[string]sets.String)
	for _, source := range sources {
		sourceName := source.GetName()
		originSourceName := getTypePodOriginSourceName(sourceName)
		nameSet, ok := sourceNames[originSourceName]
		if !ok {
			sourceNames[originSourceName] = sets.NewString(sourceName)
			continue
		}
		nameSet.Insert(sourceName)
	}

	extConfigs := make([]index.ExtInterceptorConfig, 0)
	err := cfg.UnpackRaw([]byte(interceptor), &extConfigs)
	if err != nil {
		return nil, err
	}

	for i, extConfig := range extConfigs {
		if len(extConfig.BelongTo) == 0 {
			continue
		}

		newBelongTo := make([]string, 0)
		for _, origin := range extConfig.BelongTo {
			podSrcNameSet, ok := sourceNames[origin]
			if !ok {
				continue
			}
			newBelongTo = append(newBelongTo, podSrcNameSet.List()...)
		}

		extConfigs[i].BelongTo = newBelongTo
	}

	interceptorCfgs := make([]cfg.CommonCfg, 0)
	for _, v := range extConfigs {
		commonCfg, err := cfg.Pack(v)
		if err != nil {
			log.Warn("pack interceptor config error: %+v", err)
			continue
		}
		interceptorCfgs = append(interceptorCfgs, commonCfg)
	}

	return interceptorCfgs, nil
}

func getLogConfigFromContainer(pod *corev1.Pod) []envLogConfig {
	var envConfigs []envLogConfig
	for _, container := range pod.Spec.Containers {
		config := envLogConfig{}
		for _, env := range container.Env {
			if env.Name == envSource {
				config.source = env.Value
				continue
			}

			if env.Name == envSink {
				config.sink = env.Value
				continue
			}

			if env.Name == envInterceptor {
				config.interceptor = env.Value
			}
		}

		if config.source != "" {
			config.name = container.Name
			envConfigs = append(envConfigs, config)
		}
	}
	return envConfigs
}
