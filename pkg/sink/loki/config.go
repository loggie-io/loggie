package loki

import "time"

type Config struct {
	URL         string            `yaml:"url,omitempty" validate:"required"`
	ContentType string            `yaml:"contentType,omitempty" default:"json" validate:"oneof=json protobuf"`
	TenantId    string            `yaml:"tenantId,omitempty"`
	Timeout     time.Duration     `yaml:"timeout,omitempty" default:"30s"`
	EntryLine   string            `yaml:"entryLine,omitempty"`
	Headers     map[string]string `yaml:"header,omitempty"`
}
