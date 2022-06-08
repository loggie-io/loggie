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

const (
	RuntimeDocker     = "docker"
	RuntimeContainerd = "containerd"
)

type Config struct {
	Cluster        string `yaml:"cluster" default:""`
	Kubeconfig     string `yaml:"kubeconfig"`
	Master         string `yaml:"master"`
	NodeName       string `yaml:"-"`
	ConfigFilePath string `yaml:"-"`

	ContainerRuntime string `yaml:"containerRuntime" default:"docker" validate:"oneof=docker containerd"`
	DockerDataRoot   string `yaml:"dockerDataRoot" default:"/var/lib/docker"`
	PodLogDirPrefix  string `yaml:"podLogDirPrefix" default:"/var/log/pods"`
	KubeletRootDir   string `yaml:"kubeletRootDir" default:"/var/lib/kubelet"`

	Fields      Fields `yaml:"fields"`
	ParseStdout bool   `yaml:"parseStdout"`
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
