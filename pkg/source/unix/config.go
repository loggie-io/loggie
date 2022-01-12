package unix

import "time"

type Config struct {
	Path           string        `yaml:"path,omitempty" validate:"required"`
	MaxBytes       int           `yaml:"maxBytes,omitempty" default:"40960"`
	MaxConnections int           `yaml:"maxConnections"`
	Timeout        time.Duration `yaml:"timeout" default:"5m"`
}
