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

package skafka

import (
	"github.com/Shopify/sarama"
	"regexp"
	"time"

	"github.com/pkg/errors"

	kakfasink "github.com/loggie-io/loggie/pkg/sink/skafka"
)

const (
	earliestOffsetReset = "earliest"
	latestOffsetReset   = "latest"
)

type Config struct {
	Name               string         `yaml:"name"`
	Brokers            []string       `yaml:"brokers,omitempty" validate:""`
	Topic              string         `yaml:"topic,omitempty" validate:"required"`
	GroupId            string         `yaml:"groupId,omitempty" default:"loggie"`
	Version            string         `yaml:"version" default:"0.10.2.0"`
	AutoCommitInterval time.Duration  `yaml:"autoCommitInterval" default:"1s"`
	AutoOffsetReset    string         `yaml:"autoOffsetReset" default:"latest" validate:"oneof=earliest latest"`
	EnableAutoCommit   bool           `yaml:"enableAutoCommit" default:"true"`
	SASL               kakfasink.SASL `yaml:"sasl,omitempty"`
}

func getAutoOffset(autoOffsetReset string) int64 {
	switch autoOffsetReset {
	case earliestOffsetReset:
		return sarama.OffsetOldest
	case latestOffsetReset:
		return sarama.OffsetNewest
	}

	return sarama.OffsetNewest
}

func (c *Config) Validate() error {
	if c.Topic != "" {
		_, err := regexp.Compile(c.Topic)
		if err != nil {
			return errors.WithMessagef(err, "compile kafka topic regex %s error", c.Topic)
		}
	}

	if err := c.SASL.Validate(); err != nil {
		return err
	}

	return nil
}
