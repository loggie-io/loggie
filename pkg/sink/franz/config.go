package franz

import (
	"strings"
	"time"
)

const defaultKerberosConfigPath = "/etc/krb5.conf"

type Config struct {
	Brokers      []string          `yaml:"brokers,omitempty" validate:"required"`
	Topic        string            `yaml:"topic,omitempty" validate:"required" default:"loggie"`
	Balance      string            `yaml:"balance,omitempty" default:"roundRobin"`
	BatchSize    int               `yaml:"batchSize,omitempty"`
	BatchBytes   int32             `yaml:"batchBytes,omitempty"`
	RetryTimeout time.Duration     `yaml:"retryTimeout,omitempty"`
	WriteTimeout time.Duration     `yaml:"writeTimeout,omitempty"`
	Compression  string            `yaml:"compression,omitempty" default:"gzip"`
	SASL         SASL              `yaml:"SASL,omitempty"`
	TLS          TLS               `yaml:"tls,omitempty"`
	Security     map[string]string `yaml:"security,omitempty"`
}

type SASL struct {
	Mechanism string `yaml:"mechanism,omitempty"`
	Enabled   *bool  `yaml:"enabled,omitempty"`
	UserName  string `yaml:"UserName,omitempty"`
	Password  string `yaml:"password,omitempty"`
	GSSAPI    GSSAPI `yaml:"gssapi,omitempty"`
}

type TLS struct {
	Enabled        *bool  `yaml:"enabled,omitempty"`
	CaCertFiles    string `yaml:"caCertFiles,omitempty"`
	ClientCertFile string `yaml:"clientCertFile,omitempty"`
	ClientKeyFile  string `yaml:"clientKeyFile,omitempty"`

	TrustStoreLocation string `yaml:"trustStoreLocation,omitempty"`
	TrustStorePassword string `yaml:"trustStorePassword,omitempty"`
	KeystoreLocation   string `yaml:"keystoreLocation,omitempty"`
	KeystorePassword   string `yaml:"keystorePassword,omitempty"`
	EndpIdentAlgo      string `yaml:"endpIdentAlgo,omitempty"`
}

type GSSAPI struct {
	AuthType           int    `yaml:"authType,omitempty"`
	KeyTabPath         string `yaml:"keyTabPath,omitempty"`
	KerberosConfigPath string `yaml:"kerberosConfigPath,omitempty"`
	ServiceName        string `yaml:"serviceName,omitempty"`
	UserName           string `yaml:"UserName,omitempty"`
	Password           string `yaml:"password,omitempty"`
	Realm              string `yaml:"realm,omitempty"`
	DisablePAFXFAST    *bool  `yaml:"disablePAFXFAST,omitempty"`
}

// convert java client style configuration into sinker
func (cfg *Config) convertKfkSecurity() {
	if protocol, ok := cfg.Security["security.protocol"]; ok {
		if strings.Contains(protocol, "SASL") {
			*cfg.SASL.Enabled = true
		}
		if strings.Contains(protocol, "SSL") {
			*cfg.TLS.Enabled = true
		}
	}

	if cfg.TLS.Enabled != nil && *cfg.TLS.Enabled == true {
		if endpIdentAlgo, ok := cfg.Security["ssl.endpoint.identification.algorithm"]; ok {
			cfg.TLS.EndpIdentAlgo = endpIdentAlgo
		}
		if trustStoreLocation, ok := cfg.Security["ssl.truststore.location"]; ok {
			cfg.TLS.TrustStoreLocation = trustStoreLocation
		}
		if trustStorePassword, ok := cfg.Security["ssl.truststore.password"]; ok {
			cfg.TLS.TrustStorePassword = trustStorePassword
		}
		if keyStoreLocation, ok := cfg.Security["ssl.keystore.location"]; ok {
			cfg.TLS.KeystoreLocation = keyStoreLocation
		}
		if keyStorePassword, ok := cfg.Security["ssl.keystore.password"]; ok {
			cfg.TLS.KeystorePassword = keyStorePassword
		}
	}
	if cfg.TLS.Enabled != nil && *cfg.TLS.Enabled == true {
		if mechanism, ok := cfg.Security["SASL.mechanism"]; ok {
			cfg.SASL.Mechanism = mechanism
		}
		if config, ok := cfg.Security["SASL.jaas.config"]; ok {
			configMap := readConfig(config)
			if strings.Contains(cfg.SASL.Mechanism, "SCRAM") {
				// SCRAM-SHA-256 or SCRAM-SHA-512
				if UserName, ok := configMap["UserName"]; ok {
					cfg.SASL.UserName = UserName
				}
				if password, ok := configMap["password"]; ok {
					cfg.SASL.Password = password
				}
			}
			if strings.Contains(cfg.SASL.Mechanism, "GSSAPI") {
				// GSSAPI
				if useKeyTab, ok := configMap["useKeyTab"]; ok {
					if useKeyTab == "true" {
						cfg.SASL.GSSAPI.AuthType = 2
					} else {
						cfg.SASL.GSSAPI.AuthType = 1
					}
				}
				if cfg.SASL.GSSAPI.AuthType == 1 {
					//UserName and password
					if UserName, ok := configMap["UserName"]; ok {
						cfg.SASL.GSSAPI.UserName = UserName
					}
					if password, ok := configMap["password"]; ok {
						cfg.SASL.GSSAPI.Password = password
					}
				} else {
					//Keytab
					if keyTab, ok := configMap["keyTab"]; ok {
						cfg.SASL.GSSAPI.KeyTabPath = keyTab
					}
					if principal, ok := configMap["principal"]; ok {
						UserName := strings.Split(principal, "@")[0]
						realm := strings.Split(principal, "@")[1]
						cfg.SASL.GSSAPI.UserName = UserName
						cfg.SASL.GSSAPI.Realm = realm
					}
					if servicename, ok := cfg.Security["SASL.kerberos.service.name"]; ok {
						cfg.SASL.GSSAPI.ServiceName = servicename
					}
					if cfg.SASL.GSSAPI.KerberosConfigPath == "" {
						cfg.SASL.GSSAPI.KerberosConfigPath = defaultKerberosConfigPath
					}
				}
			}
		}
	}
}

func readConfig(config string) map[string]string {
	configMap := make(map[string]string)
	config = strings.TrimSuffix(config, ";")
	fields := strings.Split(config, " ")
	for _, field := range fields {
		if strings.Contains(field, "=") {
			key := strings.Split(field, "=")[0]
			value := strings.Split(field, "=")[1]
			value = strings.Trim(value, "\"")
			configMap[key] = value
		}
	}
	return configMap
}
