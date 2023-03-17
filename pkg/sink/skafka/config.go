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
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/loggie-io/loggie/pkg/util/pattern"
)

const (
	SASLNoneType  = ""
	SASLPlainType = "plain"

	CompressionGzip   = "gzip"
	CompressionSnappy = "snappy"
	CompressionLz4    = "lz4"
	CompressionZstd   = "zstd"
)

type Config struct {
	Brokers     []string `yaml:"brokers,omitempty" validate:"required"`
	Topic       string   `yaml:"topic,omitempty" validate:"required" default:"loggie"`
	Version     string   `yaml:"version" default:"0.10.2.1"`
	Compression string   `yaml:"compression,omitempty" default:"snappy"`
	SASL        SASL     `yaml:"sasl,omitempty"`
}

type SASL struct {
	Type     string `yaml:"type,omitempty"`
	UserName string `yaml:"userName,omitempty"`
	Password string `yaml:"password,omitempty"`
}

func (c *Config) Validate() error {

	if err := pattern.Validate(c.Topic); err != nil {
		return err
	}

	if c.SASL.Type != SASLPlainType && c.SASL.Type != SASLNoneType {
		return fmt.Errorf("kafka sink sasl type %s not supported", c.SASL.Type)
	}

	if err := c.SASL.Validate(); err != nil {
		return err
	}

	return nil
}

func (s *SASL) Validate() error {
	if s.Type != SASLNoneType {
		if s.UserName == "" {
			return fmt.Errorf("kafka sink or source %s sasl with empty user name", s.Type)
		}
		if s.Password == "" {
			return fmt.Errorf("kafka sink or source %s sasl with empty password", s.Type)
		}
	}

	return nil
}

func compression(compression string) sarama.CompressionCodec {
	switch compression {
	case CompressionGzip:
		return sarama.CompressionGZIP

	case CompressionLz4:
		return sarama.CompressionLZ4

	case CompressionSnappy:
		return sarama.CompressionSnappy

	case CompressionZstd:
		return sarama.CompressionZSTD

	default:
		return sarama.CompressionNone
	}
}
