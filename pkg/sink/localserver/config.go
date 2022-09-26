package localserver

type Config struct {
	Host string `yaml:"host,omitempty" validate:"required"`
}

func (c *Config) Validate() error {
	return nil
}
