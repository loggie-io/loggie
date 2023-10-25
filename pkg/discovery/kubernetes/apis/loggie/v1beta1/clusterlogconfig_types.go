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

package v1beta1

import (
	"github.com/pkg/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	SelectorTypePod      = "pod"
	SelectorTypeNode     = "node"
	SelectorTypeCluster  = "cluster"
	SelectorTypeVm       = "vm"
	SelectorTypeWorkload = "workload"
	SelectorTypeAll      = "all"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ClusterLogConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   Spec   `json:"spec"`
	Status Status `json:"status"`
}

type Spec struct {
	Selector *Selector `json:"selector,omitempty"`
	Pipeline *Pipeline `json:"pipeline,omitempty"`
}

type Selector struct {
	Cluster           string `json:"cluster,omitempty"`
	Type              string `json:"type,omitempty"`
	PodSelector       `json:",inline"`
	NodeSelector      `json:",inline"`
	NamespaceSelector `json:",inline"`
	WorkloadSelector  []WorkloadSelector `json:"workload_selector,omitempty"`
}

type PodSelector struct {
	LabelSelector map[string]string `json:"labelSelector,omitempty"`
}

type NodeSelector struct {
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

type NamespaceSelector struct {
	NamespaceSelector        []string `json:"namespaceSelector,omitempty"`
	ExcludeNamespaceSelector []string `json:"excludeNamespaceSelector,omitempty"`
}

type WorkloadSelector struct {
	Type                     []string `json:"type,omitempty"`
	NameSelector             []string `json:"nameSelector,omitempty"`
	NamespaceSelector        []string `json:"namespaceSelector,omitempty"`
	ExcludeNamespaceSelector []string `json:"excludeNamespaceSelector,omitempty"`
}

type Pipeline struct {
	Name           string `json:"name,omitempty"`
	Sources        string `json:"sources,omitempty"`
	Sink           string `json:"sink,omitempty"`
	Interceptors   string `json:"interceptors,omitempty"`
	SinkRef        string `json:"sinkRef,omitempty"`
	InterceptorRef string `json:"interceptorRef,omitempty"`
	Queue          string `json:"queue,omitempty"`
}

type Status struct {
	Message Message `json:"message,omitempty"`
}

type Message struct {
	Reason             string `json:"reason,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
}

func (in *ClusterLogConfig) Validate() error {
	if in.Spec.Pipeline == nil {
		return errors.New("spec.pipelines is required")
	}
	if in.Spec.Selector == nil {
		return errors.New("spec.selector is required")
	}

	tp := in.Spec.Selector.Type
	if tp != SelectorTypePod && tp != SelectorTypeNode && tp != SelectorTypeCluster && tp != SelectorTypeVm && tp != SelectorTypeWorkload {
		return errors.New("spec.selector.type is invalidate")
	}

	if tp == SelectorTypeCluster && in.Spec.Selector.Cluster == "" {
		return errors.New("selector.cluster is required when selector.type=cluster")
	}

	if in.Spec.Pipeline.Sources == "" {
		return errors.New("pipeline sources is empty")
	}

	return nil
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ClusterLogConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []ClusterLogConfig `json:"items"`
}

func (in *ClusterLogConfig) ToLogConfig() *LogConfig {
	clgc := in.DeepCopy()

	lgc := &LogConfig{}
	lgc.Name = clgc.Name
	lgc.Labels = clgc.Labels
	lgc.Annotations = clgc.Annotations

	lgc.Spec = clgc.Spec

	return lgc
}
