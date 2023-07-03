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

package rocketmq

import (
	"errors"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"os"
)

type Config struct {
	// NameServer or NsResolver must be set.
	NameServer                    []string             `yaml:"nameServer,omitempty"`
	NsResolver                    []string             `yaml:"nsResolver,omitempty"`
	Topic                         string               `yaml:"topic,omitempty" validate:"required" default:"loggie"`
	IfRenderTopicFailed           RenderTopicOrTagFail `yaml:"ifRenderTopicFailed,omitempty"`
	Tag                           string               `yaml:"tag,omitempty"`
	IfRenderTagFailed             RenderTopicOrTagFail `yaml:"ifRenderTagFailed,omitempty"`
	IgnoreUnknownTopicOrPartition bool                 `yaml:"ignoreUnknownTopicOrPartition,omitempty"`
	Group                         string               `yaml:"group,omitempty" default:"DEFAULT_PRODUCER"`
	Namespace                     string               `yaml:"namespace,omitempty"`
	MessageKeys                   []string             `yaml:"messageKeys,omitempty"`
	Retry                         int                  `yaml:"retry,omitempty" default:"2" validate:"gte=0"`
	SendMsgTimeout                time.Duration        `yaml:"sendMsgTimeout,omitempty" default:"3s" validate:"gte=-1"`
	VIPChannel                    bool                 `yaml:"vipChannel,omitempty" default:"false"`
	CompressLevel                 int                  `yaml:"compressLevel,omitempty" default:"5" validate:"oneof=0 1 2 3 4 5 6 7 8 9"`
	CompressMsgBodyOverHowmuch    int                  `yaml:"compressMsgBodyOverHowmuch,omitempty" default:"4096" validate:"gte=0"`
	TopicQueueNums                int                  `yaml:"topicQueueNums,omitempty" default:"4" validate:"gt=0"`
	Credentials                   *struct {
		AccessKey     string `yaml:"accessKey,omitempty"`
		SecretKey     string `yaml:"secretKey,omitempty"`
		SecurityToken string `yaml:"securityToken,omitempty"`
	} `yaml:"credentials,omitempty"`
}

type RenderTopicOrTagFail struct {
	DropEvent    bool   `yaml:"dropEvent,omitempty" default:"true"`
	IgnoreError  bool   `yaml:"ignoreError,omitempty"`
	DefaultValue string `yaml:"defaultValue,omitempty"`
}

func (c *Config) Validate() error {
	if len(c.NameServer) == 0 && len(c.NsResolver) == 0 {
		return errors.New("no nameServer or nsResolver configured")
	}
	if c.Credentials != nil {
		if c.Credentials.AccessKey == "" && c.Credentials.SecretKey == "" {
			return errors.New("the credentials must be configured completely, the accessKey, secretKey are required")
		}
	}

	return nil
}

func getOptions(
	config *Config,
) ([]producer.Option, error) {
	options := []producer.Option{}
	if config.VIPChannel {
		options = append(options, producer.WithVIPChannel(config.VIPChannel))
	}
	if config.CompressLevel > 0 {
		options = append(options, producer.WithCompressLevel(config.CompressLevel))
	}
	if config.CompressMsgBodyOverHowmuch > 0 {
		options = append(options, producer.WithCompressMsgBodyOverHowmuch(config.CompressMsgBodyOverHowmuch))
	}
	if config.Retry > 0 {
		options = append(options, producer.WithRetry(config.Retry))
	}
	if config.NameServer != nil {
		options = append(options, producer.WithNameServer(config.NameServer))
	}
	if config.NsResolver != nil {
		options = append(options, producer.WithNsResolver(primitive.NewPassthroughResolver(config.NsResolver)))
	}
	if config.SendMsgTimeout > 0 {
		options = append(options, producer.WithSendMsgTimeout(config.SendMsgTimeout))
	}
	if config.TopicQueueNums > 0 {
		options = append(options, producer.WithDefaultTopicQueueNums(config.TopicQueueNums))
	}
	if config.Group != "" {
		options = append(options, producer.WithGroupName(config.Group))
	}
	if config.Namespace != "" {
		options = append(options, producer.WithNamespace(config.Namespace))
	}
	if config.Credentials != nil {
		options = append(options, producer.WithCredentials(primitive.Credentials{
			AccessKey:     config.Credentials.AccessKey,
			SecretKey:     config.Credentials.SecretKey,
			SecurityToken: config.Credentials.SecurityToken,
		}))
	}
	options = append(options, producer.WithInstanceName(os.Getenv("HOSTNAME")))

	return options, nil
}
