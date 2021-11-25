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
	SelectorTypePod    = "pod"
	SelectorTypeNode   = "node"
	SelectorTypeLoggie = "loggie"

	PathStdout = "stdout"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type LogConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LogConfigSpec   `json:"spec"`
	Status LogConfigStatus `json:"status"`
}

type LogConfigSpec struct {
	Selector *Selector `json:"selector,omitempty"`
	Pipeline *Pipeline `json:"pipeline,omitempty"`
}

type Selector struct {
	Cluster      string `json:"cluster,omitempty"`
	Type         string `json:"type,omitempty"`
	PodSelector  `json:",inline"`
	NodeSelector `json:",inline"`
}

type PodSelector struct {
	LabelSelector map[string]string `json:"labelSelector,omitempty"`
}

type NodeSelector struct {
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

type Pipeline struct {
	Name           string `json:"name,omitempty"`
	Sources        string `json:"sources,omitempty"`
	SinkRef        string `json:"sinkRef,omitempty"`
	InterceptorRef string `json:"interceptorRef,omitempty"`
}

type LogConfigStatus struct {
	Message Message `json:"message,omitempty"`
}

type Message struct {
	Reason             string `json:"reason,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
}

func (in *LogConfig) Validate() error {
	if in.Spec.Pipeline == nil {
		return errors.New("spec.pipelines is required")
	}
	if in.Spec.Selector == nil {
		return errors.New("spec.selector is required")
	}

	tp := in.Spec.Selector.Type
	if tp != SelectorTypePod && tp != SelectorTypeNode && tp != SelectorTypeLoggie {
		return errors.New("spec.selector.type is invalidate")
	}

	if tp == SelectorTypePod && len(in.Spec.Selector.LabelSelector) == 0 {
		return errors.New("selector.labelSelector is required when selector.type=pod")
	}

	if tp == SelectorTypeLoggie && in.Spec.Selector.Cluster == "" {
		return errors.New("selector.cluster is required when selector.type=loggie")
	}

	return nil
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type LogConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LogConfig `json:"items"`
}
