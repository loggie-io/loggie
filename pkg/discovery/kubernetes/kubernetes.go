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
	"context"
	"github.com/loggie-io/loggie/pkg/core/log"
	logconfigclientset "github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/clientset/versioned"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/controller"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/external"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/runtime"
	netutils "github.com/loggie-io/loggie/pkg/util/net"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/fields"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"

	logconfigInformer "github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/informers/externalversions"
)

type Discovery struct {
	config *controller.Config

	runtime runtime.Runtime
}

func NewDiscovery(config *controller.Config) *Discovery {
	return &Discovery{
		config: config,
	}
}

func (d *Discovery) scanRunTime() {
	if d.config.ContainerRuntime != "" {
		return
	}

	name, err := runtime.GetRunTimeName(d.config.RuntimeEndpoints)
	if err != nil {
		log.Error("%s", err)
		return
	}

	if name == "" {
		log.Warn("No runtime found, set to docker by default")
		name = runtime.RuntimeDocker
	}

	d.config.ContainerRuntime = name
}

func (d *Discovery) Start(stopCh <-chan struct{}) {
	if d.config.RootFsCollectionEnabled {
		// init runtime client
		log.Info("rootFsCollection is enabled, initializing runtime client")
		runtimeCli := runtime.Init(d.config.RuntimeEndpoints, d.config.ContainerRuntime)
		if runtimeCli == nil {
			log.Fatal("containerRuntime cannot be %s when rootFsCollection is enabled", d.config.ContainerRuntime)
		}
		d.runtime = runtimeCli

		log.Info("initial runtime %s client success", runtimeCli.Name())
	} else {
		// scan runtime name
		d.scanRunTime()
	}

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

	// In the vmMode, Loggie should be running on the virtual machine rather than Kubernetes
	if d.config.VmMode {
		d.VmModeRun(stopCh, kubeClient, logConfigClient, logConfInformerFactory, kubeInformerFactory)
		return
	}

	nodeInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(kubeClient, 0, kubeinformers.WithTweakListOptions(func(lo *metav1.ListOptions) {
		lo.FieldSelector = fields.OneTermEqualSelector("metadata.name", d.config.NodeName).String()
	}))

	ctrl := controller.NewController(d.config, kubeClient, logConfigClient, kubeInformerFactory.Core().V1().Pods(),
		logConfInformerFactory.Loggie().V1beta1().LogConfigs(), logConfInformerFactory.Loggie().V1beta1().ClusterLogConfigs(), logConfInformerFactory.Loggie().V1beta1().Sinks(),
		logConfInformerFactory.Loggie().V1beta1().Interceptors(), nodeInformerFactory.Core().V1().Nodes(), logConfInformerFactory.Loggie().V1beta1().Vms(), d.runtime)

	logConfInformerFactory.Start(stopCh)
	kubeInformerFactory.Start(stopCh)
	nodeInformerFactory.Start(stopCh)

	external.InitGlobalPodIndexer(kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer(), kubeInformerFactory.Core().V1().Pods().Lister())
	if d.config.DynamicContainerLog {
		external.InitDynamicLogIndexer()
	}
	external.Cluster = d.config.Cluster

	if err := ctrl.Run(stopCh, kubeInformerFactory.Core().V1().Pods().Informer().HasSynced,
		logConfInformerFactory.Loggie().V1beta1().LogConfigs().Informer().HasSynced,
		logConfInformerFactory.Loggie().V1beta1().ClusterLogConfigs().Informer().HasSynced,
		logConfInformerFactory.Loggie().V1beta1().Sinks().Informer().HasSynced,
		logConfInformerFactory.Loggie().V1beta1().Interceptors().Informer().HasSynced,
		logConfInformerFactory.Loggie().V1beta1().Vms().Informer().HasSynced); err != nil {
		log.Panic("Error running controller: %s", err.Error())
	}
}

func (d *Discovery) VmModeRun(stopCh <-chan struct{}, kubeClient kubeclientset.Interface, logConfigClient logconfigclientset.Interface,
	logConfInformerFactory logconfigInformer.SharedInformerFactory, kubeInformerFactory kubeinformers.SharedInformerFactory) {

	metadataName := tryToFindRelatedVm(logConfigClient, d.config.NodeName)
	if metadataName == "" {
		log.Panic("cannot find loggie agent related vm")
		return
	}
	d.config.NodeName = metadataName

	vmInformerFactory := logconfigInformer.NewSharedInformerFactoryWithOptions(logConfigClient, 0, logconfigInformer.WithTweakListOptions(func(lo *metav1.ListOptions) {
		lo.FieldSelector = fields.OneTermEqualSelector("metadata.name", d.config.NodeName).String()
	}))

	ctrl := controller.NewController(d.config, kubeClient, logConfigClient, nil,
		nil, logConfInformerFactory.Loggie().V1beta1().ClusterLogConfigs(), logConfInformerFactory.Loggie().V1beta1().Sinks(),
		logConfInformerFactory.Loggie().V1beta1().Interceptors(), nil, vmInformerFactory.Loggie().V1beta1().Vms(), d.runtime)

	logConfInformerFactory.Start(stopCh)
	kubeInformerFactory.Start(stopCh)
	vmInformerFactory.Start(stopCh)

	if err := ctrl.Run(stopCh,
		logConfInformerFactory.Loggie().V1beta1().ClusterLogConfigs().Informer().HasSynced,
		logConfInformerFactory.Loggie().V1beta1().Sinks().Informer().HasSynced,
		logConfInformerFactory.Loggie().V1beta1().Interceptors().Informer().HasSynced,
		vmInformerFactory.Loggie().V1beta1().Vms().Informer().HasSynced); err != nil {
		log.Panic("Error running controller: %s", err.Error())
	}
}

func tryToFindRelatedVm(logConfigClient logconfigclientset.Interface, nodeName string) string {
	ipList, err := netutils.GetHostIPv4()
	if err != nil {
		log.Error("cannot get host IPs: %+v", err)
		return ""
	}

	// If there is no vm name with the same name as nodeName, try to use the ip name of this node to discover vm.
	for _, ip := range ipList {
		name := strings.ReplaceAll(ip, ".", "-")
		log.Info("try to get related vm name %s", name)
		vm, err := logConfigClient.LoggieV1beta1().Vms().Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			log.Warn("get vm name %s error: %+v", name, err)
			return ""
		}
		if vm.Name != "" {
			return name
		}
	}

	return ""
}
