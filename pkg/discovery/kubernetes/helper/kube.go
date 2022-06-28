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
	"bytes"
	"context"
	"fmt"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/runtime"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	corev1listers "k8s.io/client-go/listers/core/v1"

	"github.com/loggie-io/loggie/pkg/core/log"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	logconfigLister "github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/listers/loggie/v1beta1"
)

const MatchAllToken = "*"

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

type FuncGetRelatedPod func() ([]*corev1.Pod, error)

func GetLogConfigRelatedPod(lgc *logconfigv1beta1.LogConfig, podsLister corev1listers.PodLister) ([]*corev1.Pod, error) {
	var matchExpressions []metav1.LabelSelectorRequirement
	for key, val := range lgc.Spec.Selector.LabelSelector {
		if val != MatchAllToken {
			continue
		}
		sel := metav1.LabelSelectorRequirement{
			Key:      key,
			Operator: metav1.LabelSelectorOpExists,
		}
		matchExpressions = append(matchExpressions, sel)
	}

	for k, v := range lgc.Spec.Selector.LabelSelector {
		if v == MatchAllToken {
			delete(lgc.Spec.Selector.LabelSelector, k)
		}
	}

	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels:      lgc.Spec.Selector.LabelSelector,
		MatchExpressions: matchExpressions,
	})
	if err != nil {
		return nil, errors.WithMessagef(err, "make LabelSelector error")
	}

	ret, err := podsLister.Pods(lgc.Namespace).List(selector)
	if err != nil {
		return nil, errors.WithMessagef(err, "%s/%s cannot find pod by labelSelector %#v", lgc.Namespace, lgc.Name, lgc.Spec.Selector.PodSelector.LabelSelector)
	}

	return ret, nil
}

func GetPodRelatedLogConfigs(pod *corev1.Pod, lgcLister logconfigLister.LogConfigLister) ([]*logconfigv1beta1.LogConfig, error) {
	lgcList, err := lgcLister.LogConfigs(pod.Namespace).List(labels.Everything())
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

func GetPodRelatedClusterLogConfigs(pod *corev1.Pod, clgcLister logconfigLister.ClusterLogConfigLister) ([]*logconfigv1beta1.ClusterLogConfig, error) {
	clgcList, err := clgcLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	ret := make([]*logconfigv1beta1.ClusterLogConfig, 0)
	for _, lgc := range clgcList {
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
	if len(i) <= 0 {
		return true
	}

	if len(j) <= 0 {
		return false
	}

	for key, val := range i {
		if val == MatchAllToken {
			if _, ok := j[key]; ok {
				continue
			}
			return false
		}
		if j[key] != val {
			return false
		}
	}
	return true
}

func PathsInNode(podLogDirPrefix string, kubeletRootDir string, rootFsCollectionEnabled bool, runtime runtime.Runtime,
	paths []string, pod *corev1.Pod, containerId string, containerName string) ([]string, error) {

	var nodePaths []string
	var containerRootfsPaths []string

	for _, path := range paths {
		if path == logconfigv1beta1.PathStdout {
			nodePaths = append(nodePaths, GenContainerStdoutLog(podLogDirPrefix, pod.Namespace, pod.Name, string(pod.UID), containerName)...)
			continue
		}

		volumeName, volumeMountPath, subPathRes, err := findVolumeMountsByPaths(path, pod, containerName)
		if err != nil {
			if rootFsCollectionEnabled {
				containerRootfsPaths = append(containerRootfsPaths, path)
				continue
			}

			return nil, err
		}

		nodePath, err := nodePathByContainerPath(path, pod, volumeName, volumeMountPath, subPathRes, kubeletRootDir)
		if err != nil {
			if rootFsCollectionEnabled {
				containerRootfsPaths = append(containerRootfsPaths, path)
				continue
			}

			return nil, err
		}

		nodePaths = append(nodePaths, nodePath)
	}

	// fallback to container root filesystem log collection
	if len(containerRootfsPaths) > 0 {
		// find node path in container root filesystem
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rootfsPaths, err := runtime.GetRootfsPath(ctx, containerId, containerRootfsPaths)
		if err != nil {
			return nil, err
		}
		nodePaths = append(nodePaths, rootfsPaths...)
	}

	return nodePaths, nil
}

func GenContainerStdoutLog(podLogDirPrefix string, namespace string, podName string, podUID string, containerName string) []string {
	var paths []string

	paths = append(paths, filepath.Join(podLogDirPrefix, podUID, containerName, "*.log"))
	// Kubernetes v1.14.0 Changed CRI pod log directory from /var/log/pods/UID to /var/log/pods/NAMESPACE_NAME_UID. (#74441)
	paths = append(paths, filepath.Join(podLogDirPrefix, namespace+"_"+podName+"_"+podUID, containerName, "*.log"))

	return paths
}

func findVolumeMountsByPaths(path string, pod *corev1.Pod, containerName string) (volumeName string, volumeMountPath string, subPathExprResult string, err error) {
	for _, c := range pod.Spec.Containers {
		if c.Name != containerName {
			continue
		}
		for _, volMount := range c.VolumeMounts {
			if strings.HasPrefix(path, volMount.MountPath) {

				var subPathExprRes string
				if volMount.SubPathExpr != "" {
					envVars := getEnvInPod(pod, containerName)
					envMap := envVarsToMap(envVars)
					subPathExprRes = subPathExpand(volMount.SubPathExpr, envMap)
				} else if volMount.SubPath != "" {
					subPathExprRes = volMount.SubPath
				}

				return volMount.Name, volMount.MountPath, subPathExprRes, nil
			}
		}
	}
	return "", "", "", errors.Errorf("cannot find volume mounts by path: %s", path)
}

func getEnvInPod(pod *corev1.Pod, containerName string) []corev1.EnvVar {
	var envResult []corev1.EnvVar
	if pod.Spec.Containers == nil {
		return envResult
	}

	for _, container := range pod.Spec.Containers {
		if container.Name != containerName {
			continue
		}

		for _, env := range container.Env {

			if env.Value != "" {
				envResult = append(envResult, env)
				continue
			}

			if env.ValueFrom != nil && env.ValueFrom.FieldRef != nil {
				fieldPath := env.ValueFrom.FieldRef.FieldPath

				var val string
				switch fieldPath {
				case "spec.nodeName":
					val = pod.Spec.NodeName
				case "spec.serviceAccountName":
					val = pod.Spec.ServiceAccountName
				case "status.hostIP":
					val = pod.Status.HostIP
				case "status.podIP":
					val = pod.Status.PodIP

				default:
					val = extractFieldPathAsString(pod, fieldPath)
				}

				if val != "" {
					env.Value = val
					envResult = append(envResult, env)
				}
			}
		}
	}

	return envResult
}

func extractFieldPathAsString(pod *corev1.Pod, fieldPath string) string {

	if path, subscript, ok := splitMaybeSubscriptedPath(fieldPath); ok {
		switch path {
		case "metadata.annotations":
			return pod.GetAnnotations()[subscript]
		case "metadata.labels":
			return pod.GetLabels()[subscript]
		default:
			return ""
		}
	}

	switch fieldPath {
	case "metadata.annotations":
		return formatMap(pod.GetAnnotations())
	case "metadata.labels":
		return formatMap(pod.GetLabels())
	case "metadata.name":
		return pod.GetName()
	case "metadata.namespace":
		return pod.GetNamespace()
	case "metadata.uid":
		return string(pod.GetUID())
	}

	return ""
}

func splitMaybeSubscriptedPath(fieldPath string) (string, string, bool) {
	if !strings.HasSuffix(fieldPath, "']") {
		return fieldPath, "", false
	}
	s := strings.TrimSuffix(fieldPath, "']")
	parts := strings.SplitN(s, "['", 2)
	if len(parts) < 2 {
		return fieldPath, "", false
	}
	if len(parts[0]) == 0 {
		return fieldPath, "", false
	}
	return parts[0], parts[1], true
}

func formatMap(m map[string]string) (fmtStr string) {
	keys := sets.NewString()
	for key := range m {
		keys.Insert(key)
	}
	for _, key := range keys.List() {
		fmtStr += fmt.Sprintf("%v=%q\n", key, m[key])
	}
	fmtStr = strings.TrimSuffix(fmtStr, "\n")

	return
}

const (
	operator        = '$'
	referenceOpener = '('
	referenceCloser = ')'
)

func subPathExpand(expr string, envMap map[string]string) string {
	var buf bytes.Buffer
	checkpoint := 0
	for cursor := 0; cursor < len(expr); cursor++ {
		if expr[cursor] == operator && cursor+1 < len(expr) {
			buf.WriteString(expr[checkpoint:cursor])

			read, isVar, advance := tryReadVariableName(expr[cursor+1:])

			if isVar {
				val, ok := envMap[read]
				if !ok {
					return ""
				}
				buf.WriteString(val)
			} else {
				buf.WriteString(read)
			}

			cursor += advance
			checkpoint = cursor + 1
		}
	}

	return buf.String() + expr[checkpoint:]
}

func tryReadVariableName(input string) (string, bool, int) {
	switch input[0] {
	case operator:
		return input[0:1], false, 1
	case referenceOpener:
		for i := 1; i < len(input); i++ {
			if input[i] == referenceCloser {
				return input[1:i], true, i + 1
			}
		}

		return string(operator) + string(referenceOpener), false, 1
	default:
		return (string(operator) + string(input[0])), false, 1
	}
}

func envVarsToMap(envs []corev1.EnvVar) map[string]string {
	result := map[string]string{}
	for _, env := range envs {
		result[env.Name] = env.Value
	}
	return result
}

func nodePathByContainerPath(pathPattern string, pod *corev1.Pod, volumeName string, volumeMountPath string, subPathRes string, kubeletRootDir string) (string, error) {
	for _, vol := range pod.Spec.Volumes {
		if vol.Name != volumeName {
			continue
		}

		if vol.HostPath != nil {
			return getHostPath(pathPattern, volumeMountPath, vol.HostPath.Path, subPathRes), nil
		}

		if vol.EmptyDir != nil {
			return getEmptyDirNodePath(pathPattern, pod, volumeName, volumeMountPath, kubeletRootDir, subPathRes), nil
		}

		// unsupported volume type
		return "", errors.Errorf("unsupported volume type of pod %s/%s", pod.Namespace, pod.Name)
	}
	return "", errors.Errorf("cannot find match log volume by path: %s", pathPattern)
}

// eg: @pathPattern=/var/log/test/*.log;  @volumeMountPath=/var/log; @volume=/data/log/var/log
func getHostPath(pathPattern string, volumeMountPath string, hostPath string, subPath string) string {
	pathSuffix := strings.TrimPrefix(pathPattern, volumeMountPath)
	return filepath.Join(hostPath, subPath, pathSuffix)
}

func getEmptyDirNodePath(pathPattern string, pod *corev1.Pod, volumeName string, volumeMountPath string, kubeletRootDir string, subPath string) string {
	emptyDirPath := filepath.Join(kubeletRootDir, "pods", string(pod.UID), "volumes/kubernetes.io~empty-dir", volumeName)
	pathSuffix := strings.TrimPrefix(pathPattern, volumeMountPath)
	return filepath.Join(emptyDirPath, subPath, pathSuffix)
}

func GetMatchedPodLabel(labelKeys []string, pod *corev1.Pod) map[string]string {

	if len(labelKeys) == 1 && labelKeys[0] == "*" {
		return pod.Labels
	}

	matchedLabelMap := map[string]string{}

	for _, key := range labelKeys {
		matchedLabelMap[key] = pod.Labels[key]
	}
	return matchedLabelMap
}

func GetMatchedPodAnnotation(annotationKeys []string, pod *corev1.Pod) map[string]string {

	if len(annotationKeys) == 1 && annotationKeys[0] == "*" {
		return pod.Annotations
	}

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

	if len(envKeys) == 1 && envKeys[0] == "*" {
		return containerEnvMap
	}

	matchedEnvMap := map[string]string{}
	for _, key := range envKeys {
		matchedEnvMap[key] = containerEnvMap[key]
	}
	return matchedEnvMap
}

func ExtractContainerId(containerID string) string {
	statusContainerId := strings.Split(containerID, "//")
	if len(statusContainerId) < 2 {
		return ""
	}
	return statusContainerId[1]
}
