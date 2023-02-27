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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	IndicateChinese    = "loggie-cn"
	AnnotationCnPrefix = "loggie.io/"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Vm represent a virtual machine, same as Node in Kubernetes, but we used in host outside Kubernetes Cluster.
type Vm struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VmSpec   `json:"spec"`
	Status VmStatus `json:"status"`
}

type VmSpec struct {
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type VmList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Vm `json:"items"`
}

type VmStatus struct {
	Addresses []NodeAddress `json:"addresses,omitempty"`
}

// NodeAddress contains information for the node's address.
type NodeAddress struct {
	// Node address type, one of Hostname, ExternalIP or InternalIP.
	Type string `json:"type,omitempty"`
	// The node address.
	Address string `json:"address,omitempty"`
}

func (in *Vm) ConvertChineseLabels() {
	for k, v := range in.Labels {
		if v == IndicateChinese {
			// get Chinese from annotations
			in.Labels[k] = in.Annotations[AnnotationCnPrefix+k]
		}
	}
}
