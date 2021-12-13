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
	Network             string        `yaml:"network" default:"tcp"`
	Bind                string        `yaml:"bind" default:"0.0.0.0"`
	Port                string        `yaml:"port" default:"6066"`
	Timeout             time.Duration `yaml:"timeout" default:"20s"`
	MaintenanceInterval time.Duration `yaml:"maintenanceInterval,omitempty" default:"30s"`
}
