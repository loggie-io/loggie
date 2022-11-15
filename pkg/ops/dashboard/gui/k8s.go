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

package gui

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	logconfigclientset "github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/clientset/versioned"
	kubeclientset "k8s.io/client-go/kubernetes"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

type K8sClient struct {
	KubeClient    *kubeclientset.Clientset
	LgcClient     *logconfigclientset.Clientset
	MetricsClient *metricsv.Clientset
}

func initK8sClient() *K8sClient {
	cfg := config.GetConfigOrDie()
	kubeClient, err := kubeclientset.NewForConfig(cfg)
	if err != nil {
		log.Fatal("Error building kubernetes clientSet: %s", err.Error())
	}

	logConfigClient, err := logconfigclientset.NewForConfig(cfg)
	if err != nil {
		log.Fatal("Error building logConfig clientSet: %s", err.Error())
	}

	metricClient, err := metricsv.NewForConfig(cfg)
	if err != nil {
		log.Fatal("Error building metrics clientSet: %s", err.Error())
	}

	return &K8sClient{
		KubeClient:    kubeClient,
		LgcClient:     logConfigClient,
		MetricsClient: metricClient,
	}
}
