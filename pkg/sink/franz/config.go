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
	MaxConcurrentFetches int           `yaml:"maxConcurrentFetches,omitempty" default:"3"`
	FetchMaxBytes        int32         `yaml:"fetchMaxBytes,omitempty"`
	BrokerMaxReadBytes   int32         `yaml:"brokerMaxReadBytes,omitempty"`
	Compression          string        `yaml:"compression,omitempty" default:"gzip"`
	SASL                 SASL          `yaml:"sasl,omitempty"`
	TLS                  TLS           `yaml:"tls,omitempty"`
}

type SASL struct {
	Type     string `yaml:"type,omitempty"`
	Enable   *bool  `yaml:"enable,omitempty"`
	UserName string `yaml:"userName,omitempty"`
	Password string `yaml:"password,omitempty"`
	GSSAPI   GSSAPI `yaml:"gssapi,omitempty"`
}

type TLS struct {
	Enable         *bool  `yaml:"enable,omitempty"`
	CaCertFiles    string `yaml:"caCertFiles,omitempty"`
	ClientCertFile string `yaml:"clientCertFile,omitempty"`
	ClientKeyFile  string `yaml:"clientKeyFile,omitempty"`
	EndpIdentAlgo  string `yaml:"endpIdentAlgo,omitempty"`
}

type GSSAPI struct {
	ServiceName        string `yaml:"serviceName,omitempty"`
	KerberosConfigPath string `yaml:"kerberosConfigPath,omitempty"`
	AuthType           int    `yaml:"authType,omitempty"`
	KeyTabPath         string `yaml:"keyTabPath,omitempty"`
	Realm              string `yaml:"realm,omitempty"`
	UserName           string `yaml:"userName,omitempty"`
	DisablePAFXFAST    *bool  `yaml:"disablePAFXFAST,omitempty"`
}
