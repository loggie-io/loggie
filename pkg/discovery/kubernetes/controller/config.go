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
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/runtime"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/pkg/errors"
	"net/url"
)

type Config struct {
	Cluster        string `yaml:"cluster" default:""`
	Kubeconfig     string `yaml:"kubeconfig"`
	Master         string `yaml:"master"`
	NodeName       string `yaml:"-"`
	ConfigFilePath string `yaml:"-"`

	ContainerRuntime        string   `yaml:"containerRuntime"`
	RuntimeEndpoints        []string `yaml:"runtimeEndpoints" default:"[\"unix:///run/containerd/containerd.sock\", \"unix:///var/run/dockershim.sock\", \"unix:///run/crio/crio.sock\"]"`
	RootFsCollectionEnabled bool     `yaml:"rootFsCollectionEnabled"`
	PodLogDirPrefix         string   `yaml:"podLogDirPrefix" default:"/var/log/pods"`
	KubeletRootDir          string   `yaml:"kubeletRootDir" default:"/var/lib/kubelet"`

	Fields         Fields            `yaml:"fields"`    // Deprecated: use typePodFields
	K8sFields      map[string]string `yaml:"k8sFields"` // Deprecated: use typePodFields
	TypePodFields  map[string]string `yaml:"typePodFields"`
	TypeNodeFields map[string]string `yaml:"typeNodeFields"`
	ParseStdout    bool              `yaml:"parseStdout"`

	// If set to true, it means that the pipeline configuration generated does not contain specific Pod paths and meta information.
	// These data will be dynamically obtained by the file source, thereby reducing the number of configuration changes and reloads.
	DynamicContainerLog bool `yaml:"dynamicContainerLog"`
}

// Fields Deprecated
type Fields struct {
	NodeName      string `yaml:"node.name"`
	NodeIP        string `yaml:"node.ip"`
	Namespace     string `yaml:"namespace"`
	PodName       string `yaml:"pod.name"`
	PodIP         string `yaml:"pod.ip"`
	ContainerName string `yaml:"container.name"`
	LogConfig     string `yaml:"logConfig"`
}

func (c *Config) Validate() error {
	if c.ContainerRuntime != "" {
		if c.ContainerRuntime != runtime.RuntimeDocker && c.ContainerRuntime != runtime.RuntimeContainerd &&
			c.ContainerRuntime != runtime.RuntimeNone {
			return errors.Errorf("runtime only support %s/%s", runtime.RuntimeDocker, runtime.RuntimeContainerd)
		}
	}

	if len(c.RuntimeEndpoints) != 0 {
		for _, e := range c.RuntimeEndpoints {
			u, err := url.Parse(e)
			if err != nil {
				return err
			}
			if u.Scheme != "unix" {
				return errors.New("only support unix socket endpoint")
			}
		}
	}

	if c.K8sFields != nil {
		for _, v := range c.K8sFields {
			if err := pattern.Validate(v); err != nil {
				return err
			}
		}
	}

	if c.TypePodFields != nil {
		for _, v := range c.TypePodFields {
			if err := pattern.Validate(v); err != nil {
				return err
			}
		}
	}

	if c.TypeNodeFields != nil {
		for _, v := range c.TypeNodeFields {
			if err := pattern.Validate(v); err != nil {
				return err
			}
		}
	}

	return nil
}
