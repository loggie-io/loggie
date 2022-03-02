package unix

import (
	"errors"
	"strconv"
	"time"
)

type Config struct {
	Path           string        `yaml:"path,omitempty" validate:"required"`
	MaxBytes       int           `yaml:"maxBytes,omitempty" default:"40960"`
	MaxConnections int           `yaml:"maxConnections" default:"512"`
	Timeout        time.Duration `yaml:"timeout" default:"5m"`
	Mode           string        `yaml:"mode" default:"0755"`
}

func (c *Config) Validate() error {
	if c.Mode != "" {
		parsed, err := strconv.ParseUint(c.Mode, 8, 32)
		if err != nil {
			return err
		}
		if parsed > 0777 {
			return errors.New("invalid file mode")
		}
	}

	return nil
}
