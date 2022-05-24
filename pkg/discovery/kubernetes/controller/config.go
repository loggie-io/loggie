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

	Fields Fields `yaml:"fields"`
}

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
		if c.ContainerRuntime != runtime.RuntimeDocker && c.ContainerRuntime != runtime.RuntimeContainerd {
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
	return nil
}
