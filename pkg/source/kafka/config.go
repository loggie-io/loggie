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

package kafka

// Config refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
const (
	cBrokers            = "bootstrap.servers"
	cGroupId            = "group.id"
	cEnableAutoCommit   = "enable.auto.commit"
	cAutoCommitInterval = "auto.commit.interval.ms"
	cAutoOffsetReset    = "auto.offset.reset"
)

type Config struct {
	Brokers            string   `yaml:"brokers,omitempty" validate:"required"`
	Topics             []string `yaml:"topics,omitempty" validate:"required"`
	GroupId            string   `yaml:"groupId,omitempty" default:"loggie"`
	EnableAutoCommit   bool     `yaml:"enableAutoCommit" default:"true"`
	AutoCommitInterval int      `yaml:"autoCommitInterval"`
	AutoOffsetReset    string   `yaml:"autoOffsetReset" default:"latest" validate:"oneof=smallest earliest beginning largest latest end error"`
}
