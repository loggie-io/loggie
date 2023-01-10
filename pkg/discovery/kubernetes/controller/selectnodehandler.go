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
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/source"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
)

type KubeTypeNodeExtra struct {
	TypeNodeFields KubeMetaFields `yaml:"typeNodeFields,omitempty"`
}

func GetKubeTypeNodeExtraSource(src *source.Config) (*KubeTypeNodeExtra, error) {
	extra := &KubeTypeNodeExtra{}
	if err := cfg.UnpackFromCommonCfg(src.Properties, extra).Do(); err != nil {
		return nil, err
	}
	return extra, nil
}

func (c *Controller) handleLogConfigTypeNode(lgc *logconfigv1beta1.LogConfig) error {
	// check node selector
	if lgc.Spec.Selector.NodeSelector.NodeSelector != nil {
		if !helper.LabelsSubset(lgc.Spec.Selector.NodeSelector.NodeSelector, c.nodeInfo.Labels) {
			log.Debug("logConfig %s/%s is not belong to this node", lgc.Namespace, lgc.Name)
			return nil
		}
	}

	pipRaws, err := helper.ToPipeline(lgc, c.sinkLister, c.interceptorLister)
	if err != nil {
		return errors.WithMessage(err, "convert to pipeline config failed")
	}

	if err := cfg.NewUnpack(nil, pipRaws, nil).Defaults().Validate().Do(); err != nil {
		return err
	}

	lgcKey := helper.MetaNamespaceKey(lgc.Namespace, lgc.Name)
	if err = c.typeNodeIndex.ValidateAndSetConfig(lgcKey, pipRaws.Pipelines, lgc); err != nil {
		return err
	}

	for i := range pipRaws.Pipelines {
		for _, s := range pipRaws.Pipelines[i].Sources {
			if err := c.injectTypeNodeFields(s, lgc.Name); err != nil {
				return err
			}
		}
	}

	if err = c.syncConfigToFile(logconfigv1beta1.SelectorTypeNode); err != nil {
		return errors.WithMessage(err, "failed to sync config to file")
	}
	log.Info("handle clusterLogConfig %s addOrUpdate event and sync config file success", lgc.Name)
	return nil
}

func (c *Controller) injectTypeNodeFields(src *source.Config, clusterlogconfig string) error {
	if src.Fields == nil {
		src.Fields = make(map[string]interface{})
	}

	extra, err := GetKubeTypeNodeExtraSource(src)
	if err != nil {
		return err
	}

	if len(c.extraTypeNodeFieldsPattern) > 0 {
		np := renderTypeNodeFieldsPattern(c.extraTypeNodeFieldsPattern, c.nodeInfo, clusterlogconfig)
		for k, v := range np {
			src.Fields[k] = v
		}
	}

	if len(extra.TypeNodeFields) > 0 {
		if err := extra.TypeNodeFields.validate(); err != nil {
			return err
		}
		p := extra.TypeNodeFields.initPattern()
		np := renderTypeNodeFieldsPattern(p, c.nodeInfo, clusterlogconfig)
		for k, v := range np {
			src.Fields[k] = v
		}
	}

	return nil
}

func renderTypeNodeFieldsPattern(pm map[string]*pattern.Pattern, node *corev1.Node, clusterlogconfig string) map[string]interface{} {
	fields := make(map[string]interface{}, len(pm))
	for k, p := range pm {
		res, err := p.WithK8sNode(pattern.NewTypeNodeFieldsData(node, clusterlogconfig)).Render()
		if err != nil {
			log.Warn("add extra k8s node fields %s failed: %v", k, err)
			continue
		}
		fields[k] = res
	}
	return fields
}
