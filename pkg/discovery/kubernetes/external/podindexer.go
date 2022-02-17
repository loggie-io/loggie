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

package external

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"sync"
)

const (
	byPodUidIndexer      = "byPodUid"
	byContainerIdIndexer = "byContainerId"

	IndexPodUid      = "pod.uid"
	IndexNamespace   = "namespace"
	IndexPodName     = "pod.name"
	IndexContainerId = "container.id"
)

type PodIndexer interface {
	GetByNamespaceName(namespace string, name string) (*corev1.Pod, error)
	GetByUid(uid string) (*corev1.Pod, error)
	GetByContainerId(containerId string) (*corev1.Pod, error)
}

var defaultPodIndexer PodIndexer

type CustomPodIndexer struct {
	podIndex  cache.Indexer
	podLister v1.PodLister
}

func newAddonPodIndexer(podIndexer cache.Indexer) (cache.Indexer, error) {
	index := cache.Indexers{
		byPodUidIndexer:      indexByPodUid,
		byContainerIdIndexer: indexByContainerId,
	}

	if err := podIndexer.AddIndexers(index); err != nil {
		return nil, err
	}
	return podIndexer, nil
}

func indexByPodUid(obj interface{}) ([]string, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return []string{}, nil
	}
	if len(pod.Spec.NodeName) == 0 || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return []string{}, nil
	}
	return []string{string(pod.UID)}, nil
}

func indexByContainerId(obj interface{}) ([]string, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return []string{}, nil
	}
	if len(pod.Spec.NodeName) == 0 || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return []string{}, nil
	}

	if len(pod.Status.ContainerStatuses) == 0 {
		return []string{}, nil
	}

	var containerIds []string
	for _, container := range pod.Status.ContainerStatuses {
		id := helper.ExtractContainerId(container.ContainerID)
		containerIds = append(containerIds, id)
	}

	return containerIds, nil
}

var once sync.Once

func InitGlobalPodIndexer(podIndexer cache.Indexer, podLister v1.PodLister) {
	once.Do(func() {
		defaults, err := newCustomPodIndexer(podIndexer, podLister)
		if err != nil {
			log.Error("init default pod indexer error: %v", err)
			return
		}
		defaultPodIndexer = defaults
	})
}

func newCustomPodIndexer(podIndexer cache.Indexer, podLister v1.PodLister) (PodIndexer, error) {
	indexer, err := newAddonPodIndexer(podIndexer)
	if err != nil {
		return nil, err
	}
	return &CustomPodIndexer{
		podIndex:  indexer,
		podLister: podLister,
	}, nil
}

func (c *CustomPodIndexer) GetByNamespaceName(namespace string, name string) (*corev1.Pod, error) {
	return c.podLister.Pods(namespace).Get(name)
}

func (c *CustomPodIndexer) GetByUid(uid string) (*corev1.Pod, error) {
	return c.getByIndexer(byPodUidIndexer, uid)
}

func (c *CustomPodIndexer) GetByContainerId(containerId string) (*corev1.Pod, error) {
	return c.getByIndexer(byContainerIdIndexer, containerId)
}

func (c *CustomPodIndexer) getByIndexer(indexName string, indexedVal string) (*corev1.Pod, error) {
	items, err := c.podIndex.ByIndex(indexName, indexedVal)
	if err != nil {
		return nil, err
	}

	if len(items) == 0 {
		return nil, errors.Errorf("get pod by indexer %s is empty", indexName)
	}

	pod, ok := items[0].(*corev1.Pod)
	if !ok {
		return nil, errors.Errorf("failed to type assert %+v", items[0])
	}

	return pod, nil
}

func GetByPodNamespaceName(namespace string, name string) *corev1.Pod {
	if defaultPodIndexer == nil {
		return nil
	}

	pod, err := defaultPodIndexer.GetByNamespaceName(namespace, name)
	if err != nil {
		log.Warn("Unable to get pod by namespace: %s and name: %s, err: %v", namespace, name, err)
		return nil
	}

	return pod
}

func GetByPodUid(uid string) *corev1.Pod {
	if defaultPodIndexer == nil {
		return nil
	}

	pod, err := defaultPodIndexer.GetByUid(uid)
	if err != nil {
		log.Warn("Unable to get pod by uid: %s, err: %v", uid, err)
		return nil
	}

	return pod
}

func GetByContainerId(containerId string) *corev1.Pod {
	if defaultPodIndexer == nil {
		return nil
	}

	pod, err := defaultPodIndexer.GetByContainerId(containerId)
	if err != nil {
		log.Warn("Unable to get pod by containerId: %s, err: %v", containerId, err)
		return nil
	}

	return pod
}
