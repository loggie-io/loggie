package loki

import "time"

type Config struct {
	URL        string        `yaml:"url,omitempty" validate:"required"`
	EncodeJson bool          `yaml:"encodeJson,omitempty"`
	TenantId   string        `yaml:"tenantId,omitempty"`
	Timeout    time.Duration `yaml:"timeout,omitempty" default:"30s"`
}
