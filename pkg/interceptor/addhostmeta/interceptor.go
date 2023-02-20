/*
Copyright 2023 Loggie Authors

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

package addhostmeta

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/pipeline"
	netutils "github.com/loggie-io/loggie/pkg/util/net"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/host"
)

const (
	Type = "addHostMeta"
)

func init() {
	pipeline.Register(api.INTERCEPTOR, Type, makeInterceptor)
}

func makeInterceptor(info pipeline.Info) api.Component {
	return &Interceptor{
		config:   &Config{},
		metadata: make(map[string]interface{}),
	}
}

type Interceptor struct {
	config *Config

	host     *host.InfoStat
	IPv4s    []string
	metadata map[string]interface{}
}

func (icp *Interceptor) Config() interface{} {
	return icp.config
}

func (icp *Interceptor) Category() api.Category {
	return api.INTERCEPTOR
}

func (icp *Interceptor) Type() api.Type {
	return Type
}

func (icp *Interceptor) String() string {
	return fmt.Sprintf("%s/%s", icp.Category(), icp.Type())
}

func (icp *Interceptor) Init(context api.Context) error {
	return nil
}

func (icp *Interceptor) Start() error {
	info, err := host.Info()
	if err != nil {
		return errors.WithMessagef(err, "get host info")
	}
	icp.host = info
	ips, err := netutils.GetHostIPv4()
	if err != nil {
		return err
	}
	icp.IPv4s = ips

	icp.metadata = icp.addFields()
	return nil
}

func (icp *Interceptor) Stop() {
}

func (icp *Interceptor) Intercept(invoker source.Invoker, invocation source.Invocation) api.Result {
	event := invocation.Event
	header := event.Header()

	metaClone := make(map[string]interface{})
	for k, v := range icp.metadata {
		metaClone[k] = v
	}
	header[icp.config.FieldsName] = metaClone

	return invoker.Invoke(invocation)
}

func (icp *Interceptor) Order() int {
	return icp.config.Order
}

func (icp *Interceptor) BelongTo() (componentTypes []string) {
	return icp.config.BelongTo
}

func (icp *Interceptor) IgnoreRetry() bool {
	return true
}

func (icp *Interceptor) addFields() map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range icp.config.AddFields {
		result[k] = icp.metadatas(v)
	}
	return result
}

func (icp *Interceptor) metadatas(fields string) interface{} {
	switch fields {
	case "${hostname}":
		return icp.host.Hostname

	case "${os}":
		return icp.host.OS

	case "${platform}":
		return icp.host.Platform

	case "${platformFamily}":
		return icp.host.PlatformFamily

	case "${platformVersion}":
		return icp.host.PlatformVersion

	case "${kernelVersion}":
		return icp.host.KernelVersion

	case "${kernelArch}":
		return icp.host.KernelArch

	case "${ip}":
		return icp.IPv4s
	}

	return ""
}
