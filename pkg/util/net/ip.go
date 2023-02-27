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

package net

import (
	"github.com/pkg/errors"
	"net"
)

func GetHostIPv4() ([]string, error) {
	var result []string

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, errors.WithMessage(err, "get ip address error")
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			result = append(result, ipnet.IP.String())
		}
	}
	return result, nil
}
