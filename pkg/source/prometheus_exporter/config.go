package prometheus_exporter

import (
	"net/url"
	"time"
)

type Config struct {
	Endpoints  []string      `yaml:"endpoints,omitempty" validate:"required"`
	Interval   time.Duration `yaml:"interval,omitempty" default:"30s"`
	Timeout    time.Duration `yaml:"timeout,omitempty" default:"5s"`
	BufferSize int           `yaml:"bufferSize,omitempty" default:"1000" validate:"gte=1"`
	ToJson     bool          `yaml:"toJson,omitempty"`
}

func (c *Config) Validate() error {
	// check endpoints
	for _, ep := range c.Endpoints {
		_, err := url.ParseRequestURI(ep)
		if err != nil {
			return err
		}
	}
	return nil
}
