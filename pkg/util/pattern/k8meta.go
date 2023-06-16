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

package pattern

import (
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	corev1 "k8s.io/api/core/v1"
	"strings"
)

const (
	k8sToken = "_k8s."

	AllLabelToken = "${labels.key}"
)

type TypePodFieldsData struct {
	Pod              *corev1.Pod
	ContainerName    string
	LogConfig        string
	ClusterLogConfig string
}

func NewTypePodFieldsData(pod *corev1.Pod, containerName string, lgc *logconfigv1beta1.LogConfig) *TypePodFieldsData {
	var logconfig, clusterlogconfig string
	if lgc != nil {
		if lgc.Namespace != "" {
			logconfig = lgc.Name
		} else {
			clusterlogconfig = lgc.Name
		}
	}

	return &TypePodFieldsData{
		Pod:              pod,
		ContainerName:    containerName,
		LogConfig:        logconfig,
		ClusterLogConfig: clusterlogconfig,
	}
}

type TypeNodeFieldsData struct {
	Node             *corev1.Node
	ClusterLogConfig string
}

func NewTypeNodeFieldsData(node *corev1.Node, clusterlogconfig string) *TypeNodeFieldsData {
	return &TypeNodeFieldsData{
		Node:             node,
		ClusterLogConfig: clusterlogconfig,
	}
}

type TypeVmFieldsData struct {
	Vm               *v1beta1.Vm
	ClusterLogConfig string
}

func NewTypeVmFieldsData(vm *v1beta1.Vm, clusterlogconfig string) *TypeVmFieldsData {
	return &TypeVmFieldsData{
		Vm:               vm,
		ClusterLogConfig: clusterlogconfig,
	}
}

// K8sMatcher

func IsK8sVar(key string) bool {
	return strings.HasPrefix(key, k8sToken)
}
func (p *Pattern) K8sMatcherRender(key string) string {
	field := strings.TrimLeft(key, k8sToken)

	if p.tmpK8sPodData != nil {
		return renderTypePod(p.tmpK8sPodData, field)
	}

	if p.tmpK8sNodeData != nil {
		return renderTypeNode(p.tmpK8sNodeData, field)
	}

	if p.tmpVmData != nil {
		return renderTypeVm(p.tmpVmData, field)
	}

	return ""
}

func renderTypePod(data *TypePodFieldsData, field string) string {
	switch field {
	case "pod.name":
		return data.Pod.Name

	case "pod.namespace":
		return data.Pod.Namespace

	case "pod.ip":
		return data.Pod.Status.PodIP

	case "pod.uid":
		return string(data.Pod.UID)

	case "pod.container.name":
		return data.ContainerName

	case "pod.container.id":
		for _, container := range data.Pod.Status.ContainerStatuses {
			if container.Name == data.ContainerName {
				return helper.ExtractContainerId(container.ContainerID)
			}
		}
		return ""

	case "pod.container.image":
		for _, container := range data.Pod.Status.ContainerStatuses {
			if container.Name == data.ContainerName {
				return container.Image
			}
		}
		return ""

	case "node.name":
		return data.Pod.Spec.NodeName

	case "node.ip":
		return data.Pod.Status.HostIP

	case "logconfig":
		return data.LogConfig

	case "clusterlogconfig":
		return data.ClusterLogConfig

	case "workload.kind":
		return helper.GetWorkload(data.Pod).Kind

	case "workload.name":
		return helper.GetWorkload(data.Pod).Name
	}

	return ""
}

const (
	nodeLabelPrefix      = "node.labels."
	nodeAnnotationPrefix = "node.annotations."

	vmLabelPrefix      = "vm.labels."
	vmAnnotationPrefix = "vm.annotations."
)

func renderTypeNode(data *TypeNodeFieldsData, field string) string {
	if strings.HasPrefix(field, nodeLabelPrefix) {
		key := strings.TrimPrefix(field, nodeLabelPrefix)
		return data.Node.Labels[key]
	}

	if strings.HasPrefix(field, nodeAnnotationPrefix) {
		key := strings.TrimPrefix(field, nodeAnnotationPrefix)
		return data.Node.Annotations[key]
	}

	switch field {
	case "node.name":
		return data.Node.Name

	case "clusterlogconfig":
		return data.ClusterLogConfig

	case "node.addresses.InternalIP":
		return getNodeAddress(data.Node.Status.Addresses, corev1.NodeInternalIP)

	case "node.addresses.Hostname":
		return getNodeAddress(data.Node.Status.Addresses, corev1.NodeHostName)

	case "node.nodeInfo.kernelVersion":
		return data.Node.Status.NodeInfo.KernelVersion

	case "node.nodeInfo.osImage":
		return data.Node.Status.NodeInfo.OSImage

	case "node.nodeInfo.containerRuntimeVersion":
		return data.Node.Status.NodeInfo.ContainerRuntimeVersion

	case "node.nodeInfo.kubeletVersion":
		return data.Node.Status.NodeInfo.KubeletVersion

	case "node.nodeInfo.kubeProxyVersion":
		return data.Node.Status.NodeInfo.KubeProxyVersion

	case "node.nodeInfo.operatingSystem":
		return data.Node.Status.NodeInfo.OperatingSystem

	case "node.nodeInfo.architecture":
		return data.Node.Status.NodeInfo.Architecture
	}

	return ""
}

func getNodeAddress(address []corev1.NodeAddress, addressType corev1.NodeAddressType) string {
	for _, addr := range address {
		if addr.Type == addressType {
			return addr.Address
		}
	}

	return ""
}

func renderTypeVm(data *TypeVmFieldsData, field string) string {
	if strings.HasPrefix(field, vmLabelPrefix) {
		key := strings.TrimPrefix(field, vmLabelPrefix)
		return data.Vm.Labels[key]
	}

	if strings.HasPrefix(field, vmAnnotationPrefix) {
		key := strings.TrimPrefix(field, vmAnnotationPrefix)
		return data.Vm.Annotations[key]
	}

	switch field {
	case "clusterlogconfig":
		return data.ClusterLogConfig
	}

	return ""
}
