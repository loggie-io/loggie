package prometheus_exporter

import (
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"net/url"
	"time"
)

type Config struct {
	Endpoints []string          `yaml:"endpoints,omitempty" validate:"required"`
	Interval  time.Duration     `yaml:"interval,omitempty" default:"30s"`
	Timeout   time.Duration     `yaml:"timeout,omitempty" default:"5s"`
	ToJson    bool              `yaml:"toJson,omitempty"`
	Labels    map[string]string `yaml:"labels,omitempty"`
}

func (c *Config) Validate() error {
	// check endpoints
	for _, ep := range c.Endpoints {
		_, err := url.ParseRequestURI(ep)
		if err != nil {
			return err
		}
	}

	if len(c.Labels) != 0 {
		for _, v := range c.Labels {
			if err := pattern.Validate(v); err != nil {
				return err
			}
		}
	}

	return nil
}
