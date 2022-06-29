/*
Copyright 2022 Loggie Authors

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

package addk8smeta

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/global"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/external"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/source/file"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
)

const Type = "addK8sMeta"

func init() {
	pipeline.Register(api.INTERCEPTOR, Type, makeInterceptor)
}

func makeInterceptor(info pipeline.Info) api.Component {
	return &Interceptor{
		config: &Config{},
	}
}

type Interceptor struct {
	config *Config

	Splits   []string
	Matchers []string
}

func (icp *Interceptor) Config() interface{} {
	return icp.config
}

func (icp *Interceptor) Category() api.Category {
	return api.INTERCEPTOR
}

func (icp *Interceptor) Type() api.Type {
	return Type
}

func (icp *Interceptor) String() string {
	return fmt.Sprintf("%s/%s", icp.Category(), icp.Type())
}

func (icp *Interceptor) Init(context api.Context) error {
	splits, matchers := pattern.GetSplits(icp.config.Pattern)
	icp.Splits = splits
	icp.Matchers = matchers
	log.Info("pattern splits: %v", splits)
	return nil
}

func (icp *Interceptor) Start() error {
	return nil
}

func (icp *Interceptor) Stop() {
}

func (icp *Interceptor) Intercept(invoker source.Invoker, invocation source.Invocation) api.Result {
	event := invocation.Event
	header := event.Header()

	val, err := valueOfPatternFields(event, icp.config.PatternFields)
	if err != nil {
		log.Warn("get value from pattern fields error: %v", err)
		return invoker.Invoke(invocation)
	}
	indexerValList := pattern.Extract(val, icp.Splits)
	indexerMap := make(map[string]string)
	for i, m := range icp.Matchers {
		indexerMap[m] = indexerValList[i]
	}

	pod := getPodByIndex(indexerMap)
	if pod == nil {
		log.Debug("failed get pod by indexer: %+v", indexerMap)
		return invoker.Invoke(invocation)
	}
	w := helper.GetWorkload(pod)

	fieldsMap := make(map[string]interface{})
	for k, v := range icp.config.AddFields {
		fieldsVal := getFieldsValue(v, pod, w)
		if fieldsVal == "" {
			continue
		}
		fieldsMap[k] = fieldsVal
	}
	header[icp.config.FieldsName] = fieldsMap

	return invoker.Invoke(invocation)
}

func (icp *Interceptor) Order() int {
	return icp.config.Order
}

func (icp *Interceptor) BelongTo() (componentTypes []string) {
	return icp.config.BelongTo
}

func (icp *Interceptor) IgnoreRetry() bool {
	return true
}

func valueOfPatternFields(event api.Event, patternFields string) (string, error) {
	if patternFields == "" {
		// default from file path
		state, ok := event.Meta().Get(file.SystemStateKey)
		if !ok {
			log.Debug("get systemState failed in event: %s", event.String())
			return "", errors.New("get systemState from meta is null")
		}

		stat, ok := state.(*file.State)
		if !ok {
			return "", errors.New("assert file.State failed")
		}

		return stat.Filename, nil
	}

	val, err := runtime.NewObject(event.Header()).GetPath(patternFields).String()
	if err != nil {
		return "", err
	}

	return val, nil
}

func getPodByIndex(indexMap map[string]string) *corev1.Pod {
	ns := indexMap[external.IndexNamespace]
	name := indexMap[external.IndexPodName]

	if ns != "" && name != "" {
		return external.GetByPodNamespaceName(ns, name)
	}

	podUid := indexMap[external.IndexPodUid]
	if podUid != "" {
		return external.GetByPodUid(podUid)
	}

	containerId := indexMap[external.IndexContainerId]
	if containerId != "" {
		return external.GetByContainerId(containerId)
	}

	return nil
}

func getFieldsValue(fieldsIndex string, pod *corev1.Pod, w helper.Workload) string {
	if pod == nil {
		return ""
	}

	switch fieldsIndex {
	case "${cluster}":
		return external.Cluster

	case "${node.name}":
		return global.NodeName

	case "${namespace}":
		return pod.Namespace

	case "${workload.kind}":
		return w.Kind

	case "${workload.name}":
		return w.Name

	case "${pod.uid}":
		return string(pod.UID)

	case "${pod.name}":
		return pod.Name

		// TODO add pod labels/annotations..
	}
	return ""
}
