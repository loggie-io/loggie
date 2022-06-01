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
	"github.com/loggie-io/loggie/pkg/core/log"
	"google.golang.org/grpc"
	criapi "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	"net"
	"net/url"
	"time"
)

const (
	ClientVersion = "v1alpha2"

	RuntimeDocker     = "docker"
	RuntimeContainerd = "containerd"
)

type Runtime interface {
	Name() string
	GetRootfsPath(ctx context.Context, containerId string, containerPaths []string) ([]string, error)
}

func Init(endpoints []string, defaultRuntime string) Runtime {
	runtimeName := defaultRuntime
	if defaultRuntime == "" {
		runtimeName = getRuntimeName(endpoints, defaultRuntime)
	}

	if runtimeName == RuntimeContainerd {
		return NewContainerD(endpoints)
	} else {
		return NewDocker()
	}
}

func getRuntimeName(endpoints []string, defaultRuntime string) string {
	cli, conn, err := getRuntimeClient(endpoints)
	if err != nil {
		log.Warn("initial runtime client failed: %v, use runtime: %s ", err, defaultRuntime)
		return defaultRuntime
	}
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	request := &criapi.VersionRequest{Version: ClientVersion}
	ver, err := cli.Version(context.Background(), request)
	if err != nil {
		log.Warn("request container runtime version error: %v, use runtime: %s", err, defaultRuntime)
		return defaultRuntime
	}
	return ver.RuntimeName
}

func getRuntimeClient(endpoints []string) (criapi.RuntimeServiceClient, *grpc.ClientConn, error) {
	conn, err := getConnection(endpoints)
	if err != nil {
		return nil, conn, err
	}

	runtimeClient := criapi.NewRuntimeServiceClient(conn)
	return runtimeClient, conn, nil
}

func getConnection(endpoints []string) (c *grpc.ClientConn, err error) {
	var errs error
	for _, ep := range endpoints {
		conn, err := connect(ep)
		if err != nil {
			errs = err
			continue
		}
		return conn, nil
	}
	return nil, errs
}

func connect(endpoint string) (*grpc.ClientConn, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	log.Debug("start dail %s", endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", addr)
	}

	conn, err := grpc.DialContext(ctx, u.Path, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithContextDialer(dial))
	if err != nil {
		log.Info("try dial %s failed: %v", endpoint, err)
		// try next endpoint
		return nil, err
	}
	return conn, nil
}
