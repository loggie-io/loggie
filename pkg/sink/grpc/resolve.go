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

package grpc

import (
	"fmt"

	"google.golang.org/grpc/resolver"
)

const (
	collectorScheme      = "cs"
	collectorServiceName = "collector.service.com"
)

type ClientResolver struct {
}

func NewBuilder(hosts []string) resolver.Builder {
	return &collectorBuilder{hosts: hosts}
}

type collectorBuilder struct {
	// collector server host,ip:port
	hosts []string
}

func (c *collectorBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &collectorResolver{
		target: target,
		cc:     cc,
		addressStore: map[string][]string{
			collectorServiceName: c.hosts,
		},
	}
	r.start()
	return r, nil
}

func (*collectorBuilder) Scheme() string { return collectorScheme }

type collectorResolver struct {
	target       resolver.Target
	cc           resolver.ClientConn
	addressStore map[string][]string
}

func (r *collectorResolver) start() {
	addressStr := r.addressStore[r.target.Endpoint]
	addrs := make([]resolver.Address, len(addressStr))
	for i, s := range addressStr {
		addrs[i] = resolver.Address{Addr: s}
	}
	fmt.Printf("resolver hosts: %v\n", addrs)
	r.cc.UpdateState(resolver.State{Addresses: addrs})
}
func (*collectorResolver) ResolveNow(o resolver.ResolveNowOptions) {}
func (*collectorResolver) Close()                                  {}
