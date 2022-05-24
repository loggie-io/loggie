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
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/runtime"
	"reflect"
	"time"

	"github.com/loggie-io/loggie/pkg/core/log"
	logconfigClientset "github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/clientset/versioned"
	logconfigSchema "github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/clientset/versioned/scheme"
	logconfigInformers "github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/informers/externalversions/loggie/v1beta1"
	logconfigLister "github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/listers/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/index"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"

	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	corev1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	corev1Informers "k8s.io/client-go/informers/core/v1"
	corev1Listers "k8s.io/client-go/listers/core/v1"
)

const (
	EventPod            = "pod"
	EventLogConf        = "logConfig"
	EventNode           = "node"
	EventClusterLogConf = "clusterLogConfig"
	EventSink           = "sink"
	EventInterceptor    = "interceptor"
)

// Element the item add to queue
type Element struct {
	Type         string `json:"type"` // resource type, eg: pod
	Key          string `json:"key"`  // MetaNamespaceKey, format: <namespace>/<name>
	SelectorType string `json:"selectorType"`
}

type Controller struct {
	config    *Config
	workqueue workqueue.RateLimitingInterface

	kubeClientset      kubernetes.Interface
	logConfigClientset logconfigClientset.Interface

	podsLister             corev1Listers.PodLister
	podsSynced             cache.InformerSynced
	logConfigLister        logconfigLister.LogConfigLister
	logConfigSynced        cache.InformerSynced
	clusterLogConfigLister logconfigLister.ClusterLogConfigLister
	clusterLogConfigSynced cache.InformerSynced
	sinkLister             logconfigLister.SinkLister
	sinkSynced             cache.InformerSynced
	interceptorLister      logconfigLister.InterceptorLister
	interceptorSynced      cache.InformerSynced
	nodeLister             corev1Listers.NodeLister
	nodeSynced             cache.InformerSynced

	typePodIndex     *index.LogConfigTypePodIndex
	typeClusterIndex *index.LogConfigTypeClusterIndex
	typeNodeIndex    *index.LogConfigTypeNodeIndex

	nodeLabels map[string]string

	record  record.EventRecorder
	Runtime runtime.Runtime
}

func NewController(
	config *Config,
	kubeClientset kubernetes.Interface,
	logConfigClientset logconfigClientset.Interface,
	podInformer corev1Informers.PodInformer,
	logConfigInformer logconfigInformers.LogConfigInformer,
	clusterLogConfigInformer logconfigInformers.ClusterLogConfigInformer,
	sinkInformer logconfigInformers.SinkInformer,
	interceptorInformer logconfigInformers.InterceptorInformer,
	nodeInformer corev1Informers.NodeInformer,
) *Controller {

	log.Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "loggie/" + config.NodeName})

	controller := &Controller{
		config:    config,
		workqueue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "logConfig"),

		kubeClientset:      kubeClientset,
		logConfigClientset: logConfigClientset,

		podsLister:             podInformer.Lister(),
		podsSynced:             podInformer.Informer().HasSynced,
		logConfigLister:        logConfigInformer.Lister(),
		logConfigSynced:        logConfigInformer.Informer().HasSynced,
		clusterLogConfigLister: clusterLogConfigInformer.Lister(),
		clusterLogConfigSynced: clusterLogConfigInformer.Informer().HasSynced,
		sinkLister:             sinkInformer.Lister(),
		sinkSynced:             sinkInformer.Informer().HasSynced,
		interceptorLister:      interceptorInformer.Lister(),
		interceptorSynced:      interceptorInformer.Informer().HasSynced,
		nodeLister:             nodeInformer.Lister(),
		nodeSynced:             nodeInformer.Informer().HasSynced,

		typePodIndex:     index.NewLogConfigTypePodIndex(),
		typeClusterIndex: index.NewLogConfigTypeLoggieIndex(),
		typeNodeIndex:    index.NewLogConfigTypeNodeIndex(),

		record: recorder,
	}

	log.Info("Setting up event handlers")
	utilruntime.Must(logconfigSchema.AddToScheme(scheme.Scheme))

	clusterLogConfigInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			config := obj.(*logconfigv1beta1.ClusterLogConfig)
			if config.Spec.Selector == nil {
				return
			}
			if !controller.belongOfCluster(config.Spec.Selector.Cluster) {
				return
			}

			controller.enqueue(obj, EventClusterLogConf, config.Spec.Selector.Type)
		},
		UpdateFunc: func(new, old interface{}) {
			newConfig := new.(*logconfigv1beta1.ClusterLogConfig)
			oldConfig := old.(*logconfigv1beta1.ClusterLogConfig)
			if newConfig.ResourceVersion == oldConfig.ResourceVersion {
				return
			}
			if newConfig.Generation == oldConfig.Generation {
				return
			}
			if newConfig.Spec.Selector == nil {
				return
			}
			if !controller.belongOfCluster(newConfig.Spec.Selector.Cluster) {
				return
			}

			controller.enqueue(new, EventClusterLogConf, newConfig.Spec.Selector.Type)
		},
		DeleteFunc: func(obj interface{}) {
			config, ok := obj.(*logconfigv1beta1.ClusterLogConfig)
			if !ok {
				return
			}
			if config.Spec.Selector == nil {
				return
			}
			if !controller.belongOfCluster(config.Spec.Selector.Cluster) {
				return
			}

			controller.enqueueForDelete(obj, EventClusterLogConf, config.Spec.Selector.Type)
		},
	})

	logConfigInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			config := obj.(*logconfigv1beta1.LogConfig)
			if config.Spec.Selector == nil {
				return
			}
			if !controller.belongOfCluster(config.Spec.Selector.Cluster) {
				return
			}

			controller.enqueue(obj, EventLogConf, config.Spec.Selector.Type)
		},
		UpdateFunc: func(old, new interface{}) {
			newConfig := new.(*logconfigv1beta1.LogConfig)
			oldConfig := old.(*logconfigv1beta1.LogConfig)
			if newConfig.ResourceVersion == oldConfig.ResourceVersion {
				return
			}
			if newConfig.Generation == oldConfig.Generation {
				return
			}

			if newConfig.Spec.Selector == nil {
				return
			}
			if !controller.belongOfCluster(newConfig.Spec.Selector.Cluster) {
				return
			}

			controller.enqueue(new, EventLogConf, newConfig.Spec.Selector.Type)
		},
		DeleteFunc: func(obj interface{}) {
			config, ok := obj.(*logconfigv1beta1.LogConfig)
			if !ok {
				return
			}
			if config.Spec.Selector == nil {
				return
			}
			if !controller.belongOfCluster(config.Spec.Selector.Cluster) {
				return
			}

			controller.enqueueForDelete(obj, EventLogConf, config.Spec.Selector.Type)
		},
	})

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			po := obj.(*corev1.Pod)
			if !helper.IsPodReady(po) {
				return
			}
			controller.enqueue(obj, EventPod, logconfigv1beta1.SelectorTypePod)
		},
		UpdateFunc: func(old, new interface{}) {
			newPod := new.(*corev1.Pod)
			oldPod := old.(*corev1.Pod)
			if newPod.ResourceVersion == oldPod.ResourceVersion {
				return
			}
			if !helper.IsPodReady(newPod) {
				return
			}
			controller.enqueue(new, EventPod, logconfigv1beta1.SelectorTypePod)
		},
		DeleteFunc: func(obj interface{}) {
			po := obj.(*corev1.Pod)

			log.Info("pod: %s/%s is deleting", po.Namespace, po.Name)
			controller.enqueueForDelete(obj, EventPod, logconfigv1beta1.SelectorTypePod)
		},
	})

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			controller.enqueue(obj, EventNode, logconfigv1beta1.SelectorTypeNode)
		},
		UpdateFunc: func(old, new interface{}) {
			newConfig := new.(*corev1.Node)
			oldConfig := old.(*corev1.Node)
			if newConfig.ResourceVersion == oldConfig.ResourceVersion {
				return
			}

			if reflect.DeepEqual(newConfig.Labels, oldConfig.Labels) {
				return
			}

			controller.enqueue(new, EventNode, logconfigv1beta1.SelectorTypeNode)
		},
	})

	interceptorInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			controller.enqueue(obj, EventInterceptor, logconfigv1beta1.SelectorTypeAll)
		},
		UpdateFunc: func(old, new interface{}) {
			newConfig := new.(*logconfigv1beta1.Interceptor)
			oldConfig := old.(*logconfigv1beta1.Interceptor)
			if newConfig.ResourceVersion == oldConfig.ResourceVersion {
				return
			}

			controller.enqueue(new, EventInterceptor, logconfigv1beta1.SelectorTypeAll)
		},
	})

	sinkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			controller.enqueue(obj, EventSink, logconfigv1beta1.SelectorTypeAll)
		},
		UpdateFunc: func(old, new interface{}) {
			newConfig := new.(*logconfigv1beta1.Sink)
			oldConfig := old.(*logconfigv1beta1.Sink)
			if newConfig.ResourceVersion == oldConfig.ResourceVersion {
				return
			}

			controller.enqueue(new, EventSink, logconfigv1beta1.SelectorTypeAll)
		},
	})

	return controller
}

func (c *Controller) enqueue(obj interface{}, eleType string, selectorType string) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	e := Element{
		Type:         eleType,
		Key:          key,
		SelectorType: selectorType,
	}
	c.workqueue.Add(e)
}

func (c *Controller) enqueueForDelete(obj interface{}, eleType string, selectorType string) {
	var key string
	var err error
	if key, err = cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	e := Element{
		Type:         eleType,
		Key:          key,
		SelectorType: selectorType,
	}
	c.workqueue.Add(e)
}

func (c *Controller) Run(stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	log.Info("Starting controller")

	// Wait for the caches to be synced before starting workers
	log.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.podsSynced, c.logConfigSynced, c.clusterLogConfigSynced, c.sinkSynced, c.interceptorSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	log.Info("Starting kubernetes discovery workers")

	go wait.Until(c.runWorker, time.Second, stopCh)

	<-stopCh
	log.Info("Shutting down kubernetes discovery workers")

	return nil
}

func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.workqueue.Done(obj)
		var element Element
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if element, ok = obj.(Element); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Foo resource to be synced.
		if err := c.syncHandler(element); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.workqueue.AddRateLimited(element)
			return fmt.Errorf("error syncing '%+v': %w, requeuing", element, err)
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		// log.Debug("Successfully synced '%s'", element)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

func (c *Controller) syncHandler(element Element) error {
	log.Debug("syncHandler start process: %+v", element)

	var err error
	switch element.Type {
	case EventPod:
		if err = c.reconcilePod(element.Key); err != nil {
			log.Warn("reconcile pod %s err: %+v", element.Key, err)
		}

	case EventClusterLogConf:
		if err = c.reconcileClusterLogConfig(element); err != nil {
			log.Warn("reconcile logConfig %s err: %+v", element.Key, err)
		}

	case EventLogConf:
		if err = c.reconcileLogConfig(element); err != nil {
			log.Warn("reconcile logConfig %s err: %+v", element.Key, err)
		}

	case EventNode:
		if err = c.reconcileNode(element.Key); err != nil {
			log.Warn("reconcile node %s err: %v", element.Key, err)
		}

	case EventSink:
		if err = c.reconcileSink(element.Key); err != nil {
			log.Warn("reconcile sink %s err: %v", element.Key, err)
		}

	case EventInterceptor:
		if err = c.reconcileInterceptor(element.Key); err != nil {
			log.Warn("reconcile interceptor %s err: %v", element.Key, err)
		}

	default:
		utilruntime.HandleError(fmt.Errorf("element type: %s not supported", element.Type))
		return nil
	}

	return nil
}

func (c *Controller) belongOfCluster(cluster string) bool {
	return c.config.Cluster == cluster
}
