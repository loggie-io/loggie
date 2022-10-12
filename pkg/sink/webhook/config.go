package webhook

type Config struct {
	Addr     string            `yaml:"addr,omitempty"`
	Template string            `yaml:"template,omitempty"`
	Timeout  int               `yaml:"timeout,omitempty" default:"30"`
	Headers  map[string]string `yaml:"headers,omitempty"`
}
