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
	dockerclient "github.com/docker/docker/client"
	"github.com/loggie-io/loggie/pkg/core/log"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/pkg/errors"
	"path"
)

type Docker struct {
	cli *dockerclient.Client
}

func NewDocker() *Docker {
	cli, err := dockerclient.NewEnvClient()
	if err != nil {
		// we can just panic when Loggie starting
		log.Fatal("initial docker client error: %s", err)
	}
	return &Docker{
		cli: cli,
	}
}

func (d *Docker) Name() string {
	return RuntimeDocker
}

func (d *Docker) GetRootfsPath(ctx context.Context, containerId string, containerPaths []string) ([]string, error) {
	containerJson, err := d.cli.ContainerInspect(ctx, containerId)
	if err != nil {
		log.Warn("docker inspect container: %s error: %s", containerId, err)
		return nil, err
	}
	if containerJson.GraphDriver.Name != "overlay2" {
		return nil, errors.Errorf("docker graphDriver name is not overlay2")
	}
	rootfsPrePath := containerJson.GraphDriver.Data["MergedDir"]

	var rootfsPaths []string
	for _, p := range containerPaths {
		if p == logconfigv1beta1.PathStdout {
			continue
		}

		rootfsPaths = append(rootfsPaths, path.Join(rootfsPrePath, p))
	}

	return rootfsPaths, nil
}
