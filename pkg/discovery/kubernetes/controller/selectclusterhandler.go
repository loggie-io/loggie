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
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	"github.com/pkg/errors"
)

func (c *Controller) handleLogConfigTypeCluster(lgc *logconfigv1beta1.LogConfig) error {

	pipRaws, err := helper.ToPipeline(lgc, c.sinkLister, c.interceptorLister)
	if err != nil {
		return errors.WithMessage(err, "convert to pipeline config failed")
	}

	pipRawsCopy := pipRaws.DeepCopy()
	if err := cfg.NewUnpack(nil, pipRawsCopy, nil).Defaults().Validate().Do(); err != nil {
		return err
	}

	lgcKey := helper.MetaNamespaceKey(lgc.Namespace, lgc.Name)
	if err = c.typeClusterIndex.ValidateAndSetConfig(lgcKey, pipRaws.GetPipelines(), lgc); err != nil {
		return err
	}

	if err = c.syncConfigToFile(logconfigv1beta1.SelectorTypeCluster); err != nil {
		return errors.WithMessage(err, "failed to sync config to file")
	}
	log.Info("handle logConfig %s/%s addOrUpdate event and sync config file success", lgc.Namespace, lgc.Name)
	return nil
}
