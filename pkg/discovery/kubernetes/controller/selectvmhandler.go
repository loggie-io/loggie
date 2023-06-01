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

package controller

import (
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/source"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/pkg/errors"
	"strings"
)

func (c *Controller) handleLogConfigTypeVm(lgc *logconfigv1beta1.LogConfig) error {
	if !c.config.VmMode {
		return nil
	}
	// check node selector
	if lgc.Spec.Selector.NodeSelector.NodeSelector != nil {
		if !helper.LabelsSubset(lgc.Spec.Selector.NodeSelector.NodeSelector, c.vmInfo.Labels) {
			log.Debug("clusterLogConfig %s/%s is not belong to this vm", lgc.Namespace, lgc.Name)
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
			c.injectTypeVmFields(s, lgc.Name)
		}
	}

	if err = c.syncConfigToFile(logconfigv1beta1.SelectorTypeVm); err != nil {
		return errors.WithMessage(err, "failed to sync config to file")
	}
	log.Info("handle clusterLogConfig %s addOrUpdate event and sync config file success", lgc.Name)
	return nil
}

func (c *Controller) injectTypeVmFields(src *source.Config, clusterlogconfig string) {
	if src.Fields == nil {
		src.Fields = make(map[string]interface{})
	}

	if len(c.extraTypeVmFieldsPattern) > 0 {
		for k, p := range c.extraTypeVmFieldsPattern {
			// inject all vm labels
			if strings.Contains(k, pattern.AllLabelToken) {
				c.vmInfo.ConvertChineseLabels()

				for labelKey, labelVal := range c.vmInfo.Labels {
					src.Fields[strings.Replace(k, pattern.AllLabelToken, labelKey, -1)] = labelVal
				}
				continue
			}

			res, err := p.WithVm(pattern.NewTypeVmFieldsData(c.vmInfo, clusterlogconfig)).Render()
			if err != nil {
				log.Warn("add extra vm fields %s failed: %v", k, err)
				continue
			}

			src.Fields[k] = res
		}
	}
}
