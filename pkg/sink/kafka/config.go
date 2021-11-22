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

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"loggie.io/loggie/pkg/core/log"
	"time"
)

const (
	BalanceHash       = "hash"
	BalanceRoundRobin = "roundRobin"
	BalanceLeastBytes = "leastBytes"

	CompressionGzip   = "gzip"
	CompressionSnappy = "snappy"
	CompressionLz4    = "lz4"
	CompressionZstd   = "zstd"
)

type Config struct {
	Brokers      []string      `yaml:"brokers,omitempty" validate:"required"`
	Topic        string        `yaml:"topic,omitempty" validate:"required" default:"loggie"`
	Balance      string        `yaml:"balance,omitempty" default:"roundRobin"`
	Compression  string        `yaml:"compression,omitempty" default:"gzip"`
	MaxAttempts  int           `yaml:"maxAttempts,omitempty"`
	BatchSize    int           `yaml:"batchSize,omitempty"`
	BatchBytes   int64         `yaml:"batchBytes,omitempty"`
	BatchTimeout time.Duration `yaml:"batchTimeout,omitempty"`
	ReadTimeout  time.Duration `yaml:"readTimeout,omitempty"`
	WriteTimeout time.Duration `yaml:"writeTimeout,omitempty"`
	RequiredAcks int           `yaml:"requiredAcks,omitempty"`
}

func (c *Config) Validate() error {
	if c.Balance != "" && c.Balance != BalanceHash && c.Balance != BalanceRoundRobin && c.Balance != BalanceLeastBytes {
		return fmt.Errorf("kafka sink balance %s is not supported", c.Balance)
	}

	if c.Compression != "" && c.Compression != CompressionGzip && c.Compression != CompressionLz4 && c.Compression != CompressionSnappy &&
		c.Compression != CompressionZstd {
		return fmt.Errorf("kafka sink compression %s is not suppported", c.Compression)
	}

	return nil
}

func balanceInstance(balance string) kafka.Balancer {
	switch balance {
	case BalanceHash:
		return &kafka.Hash{}
	case BalanceRoundRobin:
		return &kafka.RoundRobin{}
	case BalanceLeastBytes:
		return &kafka.LeastBytes{}
	default:
		log.Panic("kafka sink balance %s is not supported", balance)
		return nil
	}
}

func compression(compression string) kafka.Compression {
	switch compression {
	case CompressionGzip:
		return kafka.Gzip

	case CompressionLz4:
		return kafka.Lz4

	case CompressionSnappy:
		return kafka.Snappy

	case CompressionZstd:
		return kafka.Zstd

	default:
		log.Panic("kafka sink compression %s is not supported", compression)
		return kafka.Gzip
	}
}
