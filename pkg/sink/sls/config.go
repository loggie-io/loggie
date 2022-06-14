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

package sls

type Config struct {
	Endpoint        string `yaml:"endpoint,omitempty" validate:"required"`
	AccessKeyId     string `yaml:"accessKeyId,omitempty" validate:"required"`
	AccessKeySecret string `yaml:"accessKeySecret,omitempty" validate:"required"`
	SecurityToken   string `yaml:"securityToken,omitempty"`
	Project         string `yaml:"project,omitempty" validate:"required"`
	LogStore        string `yaml:"logStore,omitempty" validate:"required"`
	Topic           string `yaml:"topic,omitempty"` // empty topic is supported in sls storage
}
