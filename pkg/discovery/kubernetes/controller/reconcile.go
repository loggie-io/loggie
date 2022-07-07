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
	"github.com/goccy/go-yaml"

	"github.com/loggie-io/loggie/pkg/control"
	"github.com/loggie-io/loggie/pkg/core/log"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
)

const (
	ReasonFailed  = "syncFailed"
	ReasonSuccess = "syncSuccess"

	MessageSyncSuccess = "Sync type %s %v success"
	MessageSyncFailed  = "Sync type %s %v failed: %s"
)

func (c *Controller) reconcileClusterLogConfig(element Element) error {
	_, name, err := cache.SplitMetaNamespaceKey(element.Key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", element.Key))
		return err
	}

	clusterLogConfig, err := c.clusterLogConfigLister.Get(name)
	if kerrors.IsNotFound(err) {
		return c.reconcileClusterLogConfigDelete(element.Key, element.SelectorType)
	} else if err != nil {
		runtime.HandleError(fmt.Errorf("failed to get logconfig %s by lister", name))
		return err
	}

	err, keys := c.reconcileClusterLogConfigAddOrUpdate(clusterLogConfig)
	if len(keys) > 0 {
		msg := fmt.Sprintf(MessageSyncSuccess, clusterLogConfig.Spec.Selector.Type, keys)
		c.record.Event(clusterLogConfig, corev1.EventTypeNormal, ReasonSuccess, msg)
	}
	// no need to record failed event here because we recorded events when received pod create/update
	return err
}

func (c *Controller) reconcileLogConfig(element Element) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(element.Key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", element.Key))
		return err
	}

	logConf, err := c.logConfigLister.LogConfigs(namespace).Get(name)

	if kerrors.IsNotFound(err) {
		return c.reconcileLogConfigDelete(element.Key, element.SelectorType)
	} else if err != nil {
		runtime.HandleError(fmt.Errorf("[crd: %s/%s] failed to get logconfig by lister", namespace, name))
		return err
	}

	err, keys := c.reconcileLogConfigAddOrUpdate(logConf)
	if len(keys) > 0 {
		msg := fmt.Sprintf(MessageSyncSuccess, logConf.Spec.Selector.Type, keys)
		c.record.Event(logConf, corev1.EventTypeNormal, ReasonSuccess, msg)
	}
	return err
}

func (c *Controller) reconcilePod(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return err
	}

	pod, err := c.podsLister.Pods(namespace).Get(name)
	if kerrors.IsNotFound(err) {
		return c.reconcilePodDelete(key)
	} else if err != nil {
		runtime.HandleError(fmt.Errorf("{pod: %s/%s} failed to get pod by lister", namespace, name))
		return err
	}

	return c.reconcilePodAddOrUpdate(pod)
}

func (c *Controller) reconcileNode(name string) error {
	node, err := c.nodeLister.Get(name)
	if kerrors.IsNotFound(err) {
		log.Warn("node %s is not found", name)
		return nil
	} else if err != nil {
		runtime.HandleError(fmt.Errorf("failed to get node %s by lister", name))
		return nil
	}

	// update node labels
	n := node.DeepCopy()
	c.nodeLabels = n.Labels
	log.Info("node label %v is set", c.nodeLabels)
	return nil
}

func (c *Controller) reconcileInterceptor(name string) error {
	log.Info("start reconcile interceptor %s", name)

	_, err := c.interceptorLister.Get(name)
	if kerrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		runtime.HandleError(fmt.Errorf("failed to get interceptor %s by lister", name))
		return err
	}

	reconcile := func(lgc *logconfigv1beta1.LogConfig) error {
		if lgc == nil {
			return nil
		}

		if lgc.Spec.Pipeline.InterceptorRef == name {
			// flush pipeline config
			err, _ := c.reconcileLogConfigAddOrUpdate(lgc)
			return err
		}

		return nil
	}

	for lgcKey, pip := range c.typePodIndex.GetAllConfigMap() {
		if err := reconcile(pip.Lgc); err != nil {
			log.Info("reconcile interceptor %s and update logConfig %s error: %v", name, lgcKey, err)
		}
	}

	for lgcKey, pip := range c.typeClusterIndex.GetAllConfigMap() {
		if err := reconcile(pip.Lgc); err != nil {
			log.Info("reconcile interceptor %s and update logConfig %s error: %v", name, lgcKey, err)
		}
	}

	for lgcKey, pip := range c.typeNodeIndex.GetAllConfigMap() {
		if err := reconcile(pip.Lgc); err != nil {
			log.Info("reconcile interceptor %s and update logConfig %s error: %v", name, lgcKey, err)
		}
	}

	return nil
}

func (c *Controller) reconcileSink(name string) error {
	log.Info("start reconcile sink %s", name)

	_, err := c.sinkLister.Get(name)
	if kerrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		runtime.HandleError(fmt.Errorf("failed to get sink %s by lister", name))
		return err
	}

	reconcile := func(lgc *logconfigv1beta1.LogConfig) error {
		if lgc == nil {
			return nil
		}

		if lgc.Spec.Pipeline.SinkRef == name {
			// flush pipeline config
			err, _ := c.reconcileLogConfigAddOrUpdate(lgc)
			return err
		}

		return nil
	}

	for lgcKey, pip := range c.typePodIndex.GetAllConfigMap() {
		if err := reconcile(pip.Lgc); err != nil {
			log.Info("reconcile sink %s and update logConfig %s error: %v", name, lgcKey, err)
		}
	}

	for lgcKey, pip := range c.typeClusterIndex.GetAllConfigMap() {
		if err := reconcile(pip.Lgc); err != nil {
			log.Info("reconcile sink %s and update logConfig %s error: %v", name, lgcKey, err)
		}
	}

	for lgcKey, pip := range c.typeNodeIndex.GetAllConfigMap() {
		if err := reconcile(pip.Lgc); err != nil {
			log.Info("reconcile sink %s and update logConfig %s error: %v", name, lgcKey, err)
		}
	}

	return nil
}

func (c *Controller) reconcileClusterLogConfigAddOrUpdate(clgc *logconfigv1beta1.ClusterLogConfig) (err error, keys []string) {
	log.Info("clusterLogConfig: %s add or update event received", clgc.Name)

	if err := clgc.Validate(); err != nil {
		return err, nil
	}

	return c.handleAllTypesAddOrUpdate(clgc.ToLogConfig())
}

func (c *Controller) reconcileLogConfigAddOrUpdate(lgc *logconfigv1beta1.LogConfig) (err error, keys []string) {
	log.Info("logConfig: %s/%s add or update event received", lgc.Namespace, lgc.Name)

	if err := lgc.Validate(); err != nil {
		return err, nil
	}

	return c.handleAllTypesAddOrUpdate(lgc)
}

func (c *Controller) handleAllTypesAddOrUpdate(lgc *logconfigv1beta1.LogConfig) (err error, keys []string) {
	lgc = lgc.DeepCopy()
	switch lgc.Spec.Selector.Type {
	case logconfigv1beta1.SelectorTypePod:
		return c.handleLogConfigTypePodAddOrUpdate(lgc)

	case logconfigv1beta1.SelectorTypeNode:
		err := c.handleLogConfigTypeNode(lgc)
		return err, nil
	case logconfigv1beta1.SelectorTypeCluster:
		err := c.handleLogConfigTypeCluster(lgc)
		return err, nil
	default:
		log.Warn("logConfig %s/%s selector type is not supported", lgc.Namespace, lgc.Name)
		return errors.Errorf("logConfig %s/%s selector type is not supported", lgc.Namespace, lgc.Name), nil
	}
}

func (c *Controller) reconcileClusterLogConfigDelete(key string, selectorType string) error {
	log.Info("clusterLogConfig: %s delete event received", key)
	if err := c.handleAllTypesDelete(key, selectorType); err != nil {
		return err
	}

	log.Info("handle clusterLogConfig %s delete event and sync config file success", key)
	return nil
}

func (c *Controller) reconcileLogConfigDelete(key string, selectorType string) error {
	log.Info("logConfig: %s delete event received", key)
	if err := c.handleAllTypesDelete(key, selectorType); err != nil {
		return err
	}

	log.Info("handle logConfig %s delete event and sync config file success", key)
	return nil
}

func (c *Controller) handleAllTypesDelete(key string, selectorType string) error {
	switch selectorType {
	case logconfigv1beta1.SelectorTypePod:
		if ok := c.typePodIndex.DeletePipeConfigsByLogConfigKey(key); !ok {
			return nil
		}

	case logconfigv1beta1.SelectorTypeCluster:
		if ok := c.typeClusterIndex.DeleteConfig(key); !ok {
			return nil
		}

	case logconfigv1beta1.SelectorTypeNode:
		if ok := c.typeNodeIndex.DeleteConfig(key); !ok {
			return nil
		}

	default:
		return errors.Errorf("selector.type %s unsupported", selectorType)
	}

	// sync to file
	err := c.syncConfigToFile(selectorType)
	if err != nil {
		return errors.WithMessage(err, "sync to config file failed")
	}

	return nil
}

func (c *Controller) reconcilePodAddOrUpdate(pod *corev1.Pod) error {
	log.Debug("pod: %s/%s add or update event received", pod.Namespace, pod.Name)

	return c.handlePodAddOrUpdate(pod)
}

func (c *Controller) reconcilePodDelete(key string) error {
	log.Debug("pod: %s delete event received", key)

	// delete from index
	if ok := c.typePodIndex.DeletePipeConfigsByPodKey(key); !ok {
		return nil
	}

	// sync to file
	err := c.syncConfigToFile(logconfigv1beta1.SelectorTypePod)
	if err != nil {
		return errors.WithMessage(err, "sync config to file failed")
	}
	log.Info("handle pod %s delete event and sync config file success", key)

	return nil
}

func (c *Controller) syncConfigToFile(selectorType string) error {
	fileName := GenerateConfigName
	var cfgRaws *control.PipelineRawConfig
	switch selectorType {
	case logconfigv1beta1.SelectorTypePod:
		cfgRaws = c.typePodIndex.GetAllGroupByLogConfig()

	case logconfigv1beta1.SelectorTypeCluster:
		cfgRaws = c.typeClusterIndex.GetAll()
		fileName = GenerateTypeLoggieConfigName

	case logconfigv1beta1.SelectorTypeNode:
		cfgRaws = c.typeNodeIndex.GetAll()
		fileName = GenerateTypeNodeConfigName

	default:
		return errors.New("selector.type unsupported")
	}

	content, err := yaml.Marshal(cfgRaws)
	if err != nil {
		return err
	}
	dir := c.config.ConfigFilePath
	err = util.WriteFileOrCreate(dir, fileName, content)
	if err != nil {
		return err
	}
	return nil
}
