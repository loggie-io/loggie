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

package helper

import (
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"loggie.io/loggie/pkg/core/log"
	"path/filepath"
	"strings"

	corev1listers "k8s.io/client-go/listers/core/v1"
	logconfigv1beta1 "loggie.io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	logconfigLister "loggie.io/loggie/pkg/discovery/kubernetes/client/listers/loggie/v1beta1"
)

func IsPodReady(pod *corev1.Pod) bool {
	if pod.Status.ContainerStatuses == nil || len(pod.Status.ContainerStatuses) <= 0 {
		log.Debug("pod %s containerID is null, not ready", pod.Name)
		return false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.ContainerID == "" {
			return false
		}
	}

	return true
}

func MetaNamespaceKey(namespace string, name string) string {
	if len(namespace) > 0 {
		return namespace + "/" + name
	}
	return name
}

func GetLogConfigRelatedPod(lgc *logconfigv1beta1.LogConfig, podsLister corev1listers.PodLister) ([]*corev1.Pod, error) {

	labelSelector := lgc.Spec.Selector.PodSelector.LabelSelector
	namespace := lgc.Namespace

	if len(labelSelector) == 0 {
		return nil, errors.New("cannot find labelSelector pods, label is null")
	}

	ret, err := podsLister.Pods(namespace).List(labels.SelectorFromSet(labelSelector))
	if err != nil {
		log.Info("%s/%s cannot find pod by labelSelector %#v: %s", namespace, lgc.Name, labelSelector, err.Error())
		return nil, nil
	}

	return ret, nil
}

// TODO optimize the performance
func GetPodRelatedLogConfigs(pod *corev1.Pod, lgcLister logconfigLister.LogConfigLister) ([]*logconfigv1beta1.LogConfig, error) {
	lgcList, err := lgcLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	ret := make([]*logconfigv1beta1.LogConfig, 0)
	for _, lgc := range lgcList {
		if lgc.Spec.Selector == nil || lgc.Spec.Selector.Type != logconfigv1beta1.SelectorTypePod {
			continue
		}

		if LabelsSubset(lgc.Spec.Selector.LabelSelector, pod.Labels) {
			ret = append(ret, lgc)
		}
	}
	return ret, nil
}

// LabelsSubset checks if i is subset of j
func LabelsSubset(i map[string]string, j map[string]string) bool {
	if i == nil || j == nil {
		return false
	}

	for ikey, ival := range i {
		if j[ikey] != ival {
			return false
		}
	}
	return true
}

func PathsInNode(kubeletRootDir string, paths []string, pod *corev1.Pod, containerName string) ([]string, error) {
	nodePaths := make([]string, 0)

	for _, path := range paths {
		volumeName, volumeMountPath, err := findVolumeMountsByPaths(path, pod, containerName)
		if err != nil {
			return nil, err
		}

		nodePath, err := nodePathByContainerPath(path, pod, volumeName, volumeMountPath, kubeletRootDir)
		if err != nil {
			return nil, err
		}

		nodePaths = append(nodePaths, nodePath)
	}
	return nodePaths, nil

}

func GenDockerStdoutLog(dockerDataRoot string, containerId string) string {
	return filepath.Join(dockerDataRoot, "containers", containerId, containerId+"-json.log")
}

func GenContainerdStdoutLog(podLogDirPrefix string, namespace string, podName string, podUID string, containerName string) []string {
	var paths []string

	paths = append(paths, filepath.Join(podLogDirPrefix, podUID, containerName, "*.log"))
	// Kubernetes v1.14.0 Changed CRI pod log directory from /var/log/pods/UID to /var/log/pods/NAMESPACE_NAME_UID. (#74441)
	paths = append(paths, filepath.Join(podLogDirPrefix, namespace+"_"+podName+"_"+podUID, containerName, "*.log"))

	return paths
}

func findVolumeMountsByPaths(path string, pod *corev1.Pod, containerName string) (volumeName string, volumeMountPath string, err error) {
	for _, c := range pod.Spec.Containers {
		if c.Name != containerName {
			continue
		}
		for _, volMount := range c.VolumeMounts {
			if strings.HasPrefix(path, volMount.MountPath) {
				return volMount.Name, volMount.MountPath, nil
			}
		}

	}
	return "", "", errors.Errorf("cannot find volume mounts by path: %s", path)
}

func nodePathByContainerPath(pathPattern string, pod *corev1.Pod, volumeName string, volumeMountPath string, kubeletRootDir string) (string, error) {
	for _, vol := range pod.Spec.Volumes {
		if vol.Name != volumeName {
			continue
		}

		if vol.HostPath != nil {
			return getHostPath(pathPattern, volumeMountPath, vol.HostPath.Path), nil
		}

		if vol.EmptyDir != nil {
			return getEmptyDirNodePath(pathPattern, pod, volumeName, volumeMountPath, kubeletRootDir), nil
		}

		// unsupported volume type
		return "", errors.Errorf("unsupported volume type of pod %s/%s", pod.Namespace, pod.Name)
	}
	return "", errors.Errorf("cannot find match log volume by path: %s", pathPattern)
}

// eg: @pathPattern=/var/log/test/*.log;  @volumeMountPath=/var/log; @volume=/data/log/var/log
func getHostPath(pathPattern string, volumeMountPath string, hostPath string) string {
	pathSuffix := strings.TrimPrefix(pathPattern, volumeMountPath)
	return filepath.Join(hostPath, pathSuffix)
}

func getEmptyDirNodePath(pathPattern string, pod *corev1.Pod, volumeName string, volumeMountPath string, kubeletRootDir string) string {
	emptyDirPath := filepath.Join(kubeletRootDir, "pods", string(pod.UID), "volumes/kubernetes.io~empty-dir", volumeName)
	pathSuffix := strings.TrimPrefix(pathPattern, volumeMountPath)
	return filepath.Join(emptyDirPath, pathSuffix)
}

func GetMatchedPodLabel(labelKeys []string, pod *corev1.Pod) map[string]string {
	matchedLabelMap := map[string]string{}

	for _, key := range labelKeys {
		matchedLabelMap[key] = pod.Labels[key]
	}
	return matchedLabelMap
}

func GetMatchedPodAnnotation(annotationKeys []string, pod *corev1.Pod) map[string]string {
	matchedAnnotationMap := map[string]string{}

	for _, key := range annotationKeys {
		matchedAnnotationMap[key] = pod.Annotations[key]
	}
	return matchedAnnotationMap
}

func GetMatchedPodEnv(envKeys []string, pod *corev1.Pod, containerName string) map[string]string {
	containerEnvMap := map[string]string{}
	for _, container := range pod.Spec.Containers {
		if containerName != "" && containerName != container.Name {
			continue
		}

		for _, v := range container.Env {
			containerEnvMap[v.Name] = v.Value
		}
	}

	matchedEnvMap := map[string]string{}
	for _, key := range envKeys {
		matchedEnvMap[key] = containerEnvMap[key]
	}
	return matchedEnvMap
}
