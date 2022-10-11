package franz

import (
	"crypto/tls"
	"crypto/x509"
	krb5client "github.com/jcmturner/gokrb5/v8/client"
	krb5config "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"os"
	"strings"
)

const (
	Krb5KeytabAuth = 2
)

func getGroupBalancer(name string) kgo.GroupBalancer {
	switch name {
	case "roundRobin":
		return kgo.RoundRobinBalancer()
	case "range":
		return kgo.RangeBalancer()
	case "sticky":
		return kgo.StickyBalancer()
	case "cooperativeSticky":
		return kgo.CooperativeStickyBalancer()
	}
	return nil
}

func getCompression(name string) kgo.CompressionCodec {
	switch name {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	default:
		return kgo.NoCompression()
	}
}

func getMechanism(sasl SASL) sasl.Mechanism {
	switch sasl.Mechanism {
	case "PLAIN":
		auth := plain.Auth{
			User: sasl.UserName,
			Pass: sasl.Password,
		}
		mch := auth.AsMechanism()
		return mch
	case "SCRAM-SHA-256", "SCRAM-SHA-512":
		auth := scram.Auth{
			User: sasl.UserName,
			Pass: sasl.Password,
		}
		switch sasl.Mechanism {
		case "SCRAM-SHA-256":
			return auth.AsSha256Mechanism()
		case "SCRAM-SHA-512":
			return auth.AsSha512Mechanism()
		default:
			return nil
		}
	case "GSSAPI":
		gssapiCfg := sasl.GSSAPI
		auth := kerberos.Auth{Service: gssapiCfg.ServiceName}
		// refers to https://github.com/Shopify/sarama/blob/main/kerberos_client.go
		var krbCfg *krb5config.Config
		var kt *keytab.Keytab
		var err error
		if krbCfg, err = krb5config.Load(gssapiCfg.KerberosConfigPath); err != nil {
			return nil
		}
		if gssapiCfg.AuthType == Krb5KeytabAuth {
			if kt, err = keytab.Load(gssapiCfg.KeyTabPath); err != nil {
				return nil
			}
			auth.Client = krb5client.NewWithKeytab(sasl.UserName, gssapiCfg.Realm, kt, krbCfg, krb5client.DisablePAFXFAST(*gssapiCfg.DisablePAFXFAST))
		} else {
			auth.Client = krb5client.NewWithPassword(sasl.UserName,
				gssapiCfg.Realm, sasl.GSSAPI.Password, krbCfg, krb5client.DisablePAFXFAST(*gssapiCfg.DisablePAFXFAST))
		}
		return auth.AsMechanismWithClose()
	}

	return nil
}

// NewTLSConfig
// Refers to:
// https://medium.com/processone/using-tls-authentication-for-your-go-kafka-client-3c5841f2a625
// https://github.com/denji/golang-tls
// https://www.baeldung.com/java-keystore-truststore-difference
func NewTLSConfig(caCertFiles, clientCertFile, clientKeyFile string, insecureSkipVerify bool) (*tls.Config, error) {
	tlsConfig := tls.Config{}
	// Load client cert
	if clientCertFile != "" && clientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return &tlsConfig, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA cert
	caCertPool := x509.NewCertPool()
	for _, caCertFile := range strings.Split(caCertFiles, ",") {
		caCert, err := os.ReadFile(caCertFile)
		if err != nil {
			return &tlsConfig, err
		}
		caCertPool.AppendCertsFromPEM(caCert)
	}
	tlsConfig.RootCAs = caCertPool
	tlsConfig.InsecureSkipVerify = insecureSkipVerify
	return &tlsConfig, nil
}
