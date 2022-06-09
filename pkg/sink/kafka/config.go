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
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"time"

	"github.com/loggie-io/loggie/pkg/core/log"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const (
	BalanceHash       = "hash"
	BalanceRoundRobin = "roundRobin"
	BalanceLeastBytes = "leastBytes"

	CompressionGzip   = "gzip"
	CompressionSnappy = "snappy"
	CompressionLz4    = "lz4"
	CompressionZstd   = "zstd"

	SASLNoneType  = ""
	SASLPlainType = "plain"
	SASLSCRAMType = "scram"

	AlgorithmSHA256 = "sha256"
	AlgorithmSHA512 = "sha512"
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
	SASL         SASL          `yaml:"sasl,omitempty"`
}

type SASL struct {
	Type      string `yaml:"type,omitempty"`
	UserName  string `yaml:"userName,omitempty"`
	Password  string `yaml:"password,omitempty"`
	Algorithm string `yaml:"algorithm,omitempty"`
}

func (c *Config) Validate() error {

	if err := pattern.Validate(c.Topic); err != nil {
		return err
	}

	if c.Balance != "" && c.Balance != BalanceHash && c.Balance != BalanceRoundRobin && c.Balance != BalanceLeastBytes {
		return fmt.Errorf("kafka sink balance %s is not supported", c.Balance)
	}

	if c.Compression != "" && c.Compression != CompressionGzip && c.Compression != CompressionLz4 && c.Compression != CompressionSnappy &&
		c.Compression != CompressionZstd {
		return fmt.Errorf("kafka sink compression %s is not suppported", c.Compression)
	}

	if c.SASL.Type != SASLPlainType && c.SASL.Type != SASLSCRAMType && c.SASL.Type != SASLNoneType {
		return fmt.Errorf("kafka sink sasl type %s not supported", c.SASL.Type)
	}

	if c.SASL.Type != SASLNoneType {
		if c.SASL.UserName == "" {
			return fmt.Errorf("kafka sink %s sasl with empty user name", c.SASL.Type)
		}
		if c.SASL.Password == "" {
			return fmt.Errorf("kafka sink %s sasl with empty password", c.SASL.Type)
		}

		if c.SASL.Type == SASLSCRAMType {
			if c.SASL.Algorithm != "" && c.SASL.Algorithm != AlgorithmSHA512 && c.SASL.Algorithm != AlgorithmSHA256 {
				return fmt.Errorf("kafka sink %s sasl hash algorithm %s not supported", c.SASL.Type, c.SASL.Algorithm)
			}
		}
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
		log.Warn("kafka sink balance %s is not supported", balance)
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
		log.Warn("kafka sink compression %s is not supported", compression)
		return kafka.Gzip
	}
}

func mechanism(saslType, userName, password, algo string) (sasl.Mechanism, error) {
	switch saslType {
	case SASLPlainType:
		return plain.Mechanism{
			Username: userName,
			Password: password,
		}, nil
	case SASLSCRAMType:
		return scram.Mechanism(algorithm(algo), userName, password)
	default:
		return nil, nil
	}
}

func algorithm(algo string) scram.Algorithm {
	switch algo {
	case AlgorithmSHA256:
		return scram.SHA256
	case AlgorithmSHA512:
		return scram.SHA512
	default:
		log.Warn("kafka sink sasl scram hash algo %s is not supported", algo)
		return scram.SHA512
	}
}
