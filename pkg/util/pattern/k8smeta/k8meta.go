package k8smeta

import (
	corev1 "k8s.io/api/core/v1"
)

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
