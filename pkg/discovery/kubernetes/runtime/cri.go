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

package runtime

import (
	"context"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/log"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/util/json"
	"github.com/pkg/errors"
	criapi "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	"path"
)

type RuntimeType string

const (
	KataRuntimeType RuntimeType = "io.containerd.kata.v2"
	RuncRuntimeType RuntimeType = "io.containerd.runc.v2"
)

type ContainerD struct {
	cli criapi.RuntimeServiceClient
}

func NewContainerD(endpoints []string) *ContainerD {
	cli, _, err := getRuntimeClient(endpoints)
	if err != nil {
		log.Fatal("initial CRI client error: %s", err)
	}
	return &ContainerD{
		cli: cli,
	}
}

func (c *ContainerD) Name() string {
	return RuntimeContainerd
}

func (c *ContainerD) Client() interface{} {
	return c.cli
}

func (c *ContainerD) getKataRuntimeRootfsPath(infoMap map[string]any, containerId string) (string, error) {
	sandboxID, ok := infoMap["sandboxID"]
	if !ok {
		im, _ := json.Marshal(infoMap)
		log.Debug("get info map: %s from container(id: %s)", string(im), containerId)
		return "", errors.Errorf("cannot get sandboxID from container(id: %s) status", containerId)
	}
	//refer to https://github.com/kata-containers/kata-containers/blob/main/src/runtime/virtcontainers/kata_agent.go#L83
	rootfs := fmt.Sprintf("/run/kata-containers/shared/sandboxes/%v/shared/%v/rootfs", sandboxID, containerId)
	return rootfs, nil
}

func (c *ContainerD) getRuncRuntimeRootfsPath(infoMap map[string]any, containerId string) (string, error) {
	pid, ok := infoMap["pid"]
	if !ok {
		if log.IsDebugLevel() {
			im, _ := json.Marshal(infoMap)
			log.Debug("get info map: %s from container(id: %s)", string(im), containerId)
		}
		return "", errors.Errorf("cannot get pid from container(id: %s) status", containerId)
	}

	return fmt.Sprintf("/proc/%.0f/root", pid), nil
}

func (c *ContainerD) GetRootfsPath(ctx context.Context, containerId string, containerPaths []string) ([]string, error) {
	request := &criapi.ContainerStatusRequest{
		ContainerId: containerId,
		Verbose:     true,
	}

	response, err := c.cli.ContainerStatus(ctx, request)
	if err != nil {
		return nil, errors.WithMessagef(err, "get container(id: %s) status failed", containerId)
	}

	infoStr, ok := response.GetInfo()["info"]
	if !ok {
		if log.IsDebugLevel() {
			info, _ := json.Marshal(response.GetInfo())
			log.Debug("get info: %s from container(id: %s)", string(info), containerId)
		}
		return nil, errors.Errorf("cannot get info from container(id: %s) status", containerId)
	}

	infoMap := make(map[string]interface{})
	if err := json.Unmarshal([]byte(infoStr), &infoMap); err != nil {
		return nil, errors.WithMessagef(err, "get pid from container(id: %s)", containerId)
	}
	runtime, ok := infoMap["runtimeType"]
	if !ok {
		return nil, errors.Errorf("can not get runtime type from container(id: %s) status", containerId)
	}
	var prefix string
	if runtime == string(KataRuntimeType) {
		prefix, err = c.getKataRuntimeRootfsPath(infoMap, containerId)
		if err != nil {
			return nil, err
		}
	} else {
		prefix, err = c.getRuncRuntimeRootfsPath(infoMap, containerId)
		if err != nil {
			return nil, err
		}
	}

	var rootfsPaths []string
	for _, p := range containerPaths {
		if p == logconfigv1beta1.PathStdout {
			continue
		}
		rootfsPaths = append(rootfsPaths, path.Join(prefix, p))
	}

	return rootfsPaths, nil
}
