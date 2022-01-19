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

package kubernetes

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	logconfigclientset "github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/clientset/versioned"

	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/controller"
	"k8s.io/apimachinery/pkg/fields"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"

	logconfigInformer "github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/informers/externalversions"
)

type Discovery struct {
	config *controller.Config
}

func NewDiscovery(config *controller.Config) *Discovery {
	return &Discovery{
		config: config,
	}
}

func (d *Discovery) Start(stopCh <-chan struct{}) {
	cfg, err := clientcmd.BuildConfigFromFlags(d.config.Master, d.config.Kubeconfig)
	if err != nil {
		log.Panic("Error building kubeconfig: %s, cfg: %+v", err.Error(), cfg)
	}

	kubeClient, err := kubeclientset.NewForConfig(cfg)
	if err != nil {
		log.Panic("Error building kubernetes clientset: %s", err.Error())
	}

	logConfigClient, err := logconfigclientset.NewForConfig(cfg)
	if err != nil {
		log.Panic("Error building logConf clientset: %s", err.Error())
	}

	logConfInformerFactory := logconfigInformer.NewSharedInformerFactory(logConfigClient, 0)


	kubeInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(kubeClient, 0, kubeinformers.WithTweakListOptions(func(lo *metav1.ListOptions) {
		lo.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", d.config.NodeName).String()
	}))

	nodeInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(kubeClient, 0, kubeinformers.WithTweakListOptions(func(lo *metav1.ListOptions) {
		lo.FieldSelector = fields.OneTermEqualSelector("metadata.name", d.config.NodeName).String()
	}))

	ctrl := controller.NewController(d.config, kubeClient, logConfigClient, kubeInformerFactory.Core().V1().Pods(),
		logConfInformerFactory.Loggie().V1beta1().LogConfigs(), logConfInformerFactory.Loggie().V1beta1().ClusterLogConfigs(), logConfInformerFactory.Loggie().V1beta1().Sinks(),
		logConfInformerFactory.Loggie().V1beta1().Interceptors(), nodeInformerFactory.Core().V1().Nodes())

	logConfInformerFactory.Start(stopCh)
	kubeInformerFactory.Start(stopCh)
	nodeInformerFactory.Start(stopCh)

	if err := ctrl.Run(stopCh); err != nil {
		log.Panic("Error running controller: %s", err.Error())
	}
}
