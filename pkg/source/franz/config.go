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

package franz

import (
	"github.com/loggie-io/loggie/pkg/sink/franz"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"regexp"
	"time"
)

const (
	earliestOffsetReset = "earliest"
	latestOffsetReset   = "latest"
)

type Config struct {
	Brokers  []string `yaml:"brokers,omitempty" validate:"required"`
	Topic    string   `yaml:"topic,omitempty"`
	Topics   []string `yaml:"topics,omitempty"`
	GroupId  string   `yaml:"groupId,omitempty" default:"loggie"`
	ClientId string   `yaml:"clientId,omitempty"`
	Worker   int      `yaml:"worker,omitempty" default:"1"`

	FetchMaxWait           time.Duration `yaml:"fetchMaxWait,omitempty"`
	FetchMaxBytes          int32         `yaml:"fetchMaxBytes,omitempty"`
	FetchMinBytes          int32         `yaml:"fetchMinBytes,omitempty"`
	FetchMaxPartitionBytes int32         `yaml:"fetchMaxPartitionBytes,omitempty"`

	EnableAutoCommit   bool          `yaml:"enableAutoCommit,omitempty"`
	AutoCommitInterval time.Duration `yaml:"autoCommitInterval,omitempty" default:"1s"`
	AutoOffsetReset    string        `yaml:"autoOffsetReset,omitempty" default:"latest"`

	SASL franz.SASL `yaml:"sasl,omitempty"`
	TLS  franz.TLS  `yaml:"tls,omitempty"`

	AddonMeta *bool `yaml:"addonMeta,omitempty" default:"true"`
}

func getAutoOffset(autoOffsetReset string) kgo.Offset {
	switch autoOffsetReset {
	case earliestOffsetReset:
		return kgo.NewOffset().AtStart()
	case latestOffsetReset:
		return kgo.NewOffset().AtEnd()
	}

	return kgo.NewOffset().AtEnd()
}

func (c *Config) Validate() error {
	if c.Topic == "" && len(c.Topics) == 0 {
		return errors.New("topic or topics is required")
	}

	if c.Topic != "" {
		_, err := regexp.Compile(c.Topic)
		if err != nil {
			return errors.WithMessagef(err, "compile kafka topic regex %s error", c.Topic)
		}
	}
	if len(c.Topics) > 0 {
		for _, t := range c.Topics {
			_, err := regexp.Compile(t)
			if err != nil {
				return errors.WithMessagef(err, "compile kafka topic regex %s error", t)
			}
		}
	}

	if c.AutoOffsetReset != "" {
		if c.AutoOffsetReset != earliestOffsetReset && c.AutoOffsetReset != latestOffsetReset {
			return errors.Errorf("autoOffsetReset must be one of %s or %s", earliestOffsetReset, latestOffsetReset)
		}
	}

	return nil
}
