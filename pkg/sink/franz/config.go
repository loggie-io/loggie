package franz

import "time"

type Config struct {
	Brokers              []string      `yaml:"brokers,omitempty" validate:"required"`
	Topic                string        `yaml:"topic,omitempty" validate:"required" default:"loggie"`
	Balance              string        `yaml:"balance,omitempty" default:"roundRobin"`
	BatchSize            int           `yaml:"batchSize,omitempty"`
	BatchBytes           int32         `yaml:"batchBytes,omitempty"`
	RetryTimeout         time.Duration `yaml:"retryTimeout,omitempty"`
	WriteTimeout         time.Duration `yaml:"writeTimeout,omitempty"`
	MaxConcurrentFetches int
	FetchMaxBytes        int32
	BrokerMaxReadBytes   int32
	Compression          string `yaml:"compression,omitempty" default:"gzip"`
	SASL                 SASL   `yaml:"sasl,omitempty"`
	TLS                  TLS
}

type SASL struct {
	Type     string `yaml:"type,omitempty"`
	Enable   *bool
	UserName string `yaml:"userName,omitempty"`
	Password string `yaml:"password,omitempty"`
	GSSAPI   GSSAPI
}

type TLS struct {
	Enable         *bool
	CaCertFiles    string
	ClientCertFile string
	ClientKeyFile  string
	EndpIdentAlgo  string
}

type GSSAPI struct {
	ServiceName        string
	KerberosConfigPath string
	AuthType           int
	KeyTabPath         string
	Realm              string
	UserName           string
	DisablePAFXFAST    *bool
}
