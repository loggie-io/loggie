package k8smeta

import (
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	corev1 "k8s.io/api/core/v1"
	"strings"
)

const k8sToken = "_k8s."

type FieldsData struct {
	Pod           *corev1.Pod
	ContainerName string
	LogConfig     string
}

func NewFieldsData(pod *corev1.Pod, containerName string, logConfig string) *FieldsData {
	return &FieldsData{
		Pod:           pod,
		ContainerName: containerName,
		LogConfig:     logConfig,
	}
}

// K8sMatcher

func IsK8sVar(key string) bool {
	return strings.HasPrefix(key, k8sToken)
}
func K8sMatcherRender(data *FieldsData, key string) string {
	if data == nil {
		return ""
	}

	field := strings.TrimLeft(key, k8sToken)
	switch field {
	case "pod.name":
		return data.Pod.Name

	case "pod.namespace":
		return data.Pod.Namespace

	case "pod.ip":
		return data.Pod.Status.PodIP

	case "pod.container.name":
		return data.ContainerName

	case "node.name":
		return data.Pod.Spec.NodeName

	case "node.ip":
		return data.Pod.Status.HostIP

	case "logconfig":
		return data.LogConfig

	case "workload.kind":
		return helper.GetWorkload(data.Pod).Kind

	case "workload.name":
		return helper.GetWorkload(data.Pod).Name
	}

	return ""
}
