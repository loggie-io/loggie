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

import "time"

type Config struct {
	Host           string        `yaml:"host,omitempty" validate:"required"`
	LoadBalance    string        `yaml:"loadBalance,omitempty" default:"round_robin"`
	Timeout        time.Duration `yaml:"timeout,omitempty" default:"30s"`
	GrpcHeaderKey  string        `yaml:"grpcHeaderKey,omitempty"`
	AvgMessageSize int           `yaml:"avgMessageSize,omitempty" default:"65536"` // default 64k
}
