package pulsar

import (
	"errors"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/sirupsen/logrus"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type pulsarConfig struct {
	// Configure the service URL for the Pulsar service. If you have multiple brokers, you can set multiple Pulsar cluster addresses for a client.This parameter is required.
	URL   string `yaml:"url,omitempty" validate:"required"`
	Topic string `yaml:"topic,omitempty" validate:"required"`
	// Set the operation timeout. Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the operation will be marked as failed
	OperationTimeoutSeconds    time.Duration `yaml:"operationTimeoutSeconds,omitempty" default:"30s" validate:"gt=0"`
	UseTLS                     bool          `yaml:"useTLS,omitempty"`
	TLSTrustCertsFilePath      string        `yaml:"tlsTrustCertsFilePath,omitempty"`
	TLSAllowInsecureConnection bool          `yaml:"tlsAllowInsecureConnection,omitempty"`
	// Max number of connections to a single broker that is kept in the pool. default 1
	MaxConnectionsPerBroker int    `yaml:"maxConnectionPerBroker,omitempty"`
	LogLevel                string `yaml:"logLevel,omitempty" default:"info" validate:"oneof=info debug error"`

	CertificatePath string `yaml:"certificatePath,omitempty"`
	PrivateKeyPath  string `yaml:"privateKeyPath,omitempty"`
	Token           string `yaml:"token,omitempty"`
	TokenFilePath   string `yaml:"tokenFilePath,omitempty"`
	// Timeout for the establishment of a TCP connection
	ConnectionTimeout time.Duration `yaml:"connectionTimeout,omitempty" default:"5s" validate:"gt=0"`

	// producer name
	Name       string            `yaml:"name,omitempty"`
	Properties map[string]string `yaml:"properties,omitempty"`
	// SendTimeout set the timeout for a message that is not acknowledged by the server 30s.default 30s
	SendTimeout time.Duration `yaml:"sendTimeout,omitempty" default:"30s" validate:"gte=-1"`
	// MaxPendingMessages set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.
	MaxPendingMessages int                  `yaml:"maxPendingMessages,omitempty" default:"2048" validate:"gt=0"`
	HashingScheme      pulsar.HashingScheme `yaml:"hashingSchema,omitempty" default:"0" validate:"oneof=0 1"`
	// Pulsar message compression type 0:noCompression,1:LZ4,2:zlib,3:zSTD
	CompressionType pulsar.CompressionType `yaml:"compressionType,omitempty" default:"0" validate:"oneof=0 1 2 3"`
	// 0:Default 1:Faster 2:Better
	CompressionLevel        pulsar.CompressionLevel `yaml:"compressionLevel,omitempty" default:"0" validate:"oneof=0 1 2"`
	BatchingMaxPublishDelay time.Duration           `yaml:"batchingMaxPublishDelay,omitempty" default:"10ms" validate:"gt=0"`
	BatchingMaxMessages     uint                    `yaml:"batchingMaxMessages,omitempty" default:"1000" validate:"gt=0"`
	// BatchingMaxSize specifies the maximum number of bytes permitted in a batch. (default 2048 KB)
	// If set to a value greater than 1, messages will be queued until this threshold is reached or
	// BatchingMaxMessages (see above) has been reached or the batch interval has elapsed.
	BatchingMaxSize uint `yaml:"batchingMaxSize,omitempty" default:"2048" validate:"gt=0"`
}

func (c *pulsarConfig) Validate() error {
	if c.UseTLS {
		if len(c.TLSTrustCertsFilePath) == 0 {
			return errors.New("no tls_trust_certs_file_path configured")
		}
		if len(c.CertificatePath) > 0 {
			if len(c.PrivateKeyPath) == 0 {
				return errors.New("no private_key_path configured")
			}
		}
	}
	return nil
}

func getOptions(
	config *pulsarConfig,
) (*pulsar.ClientOptions, *pulsar.ProducerOptions, error) {
	clientOptions := pulsar.ClientOptions{
		URL:               config.URL,
		OperationTimeout:  config.OperationTimeoutSeconds,
		ConnectionTimeout: config.ConnectionTimeout,
	}
	if config.UseTLS {
		clientOptions.TLSTrustCertsFilePath = config.TLSTrustCertsFilePath
		if len(config.CertificatePath) > 0 {
			clientOptions.Authentication = pulsar.NewAuthenticationTLS(config.CertificatePath, config.PrivateKeyPath)
		}
	}
	if len(config.Token) > 0 {
		clientOptions.Authentication = pulsar.NewAuthenticationToken(config.Token)
	}
	if len(config.TokenFilePath) > 0 {
		clientOptions.Authentication = pulsar.NewAuthenticationTokenFromFile(config.TokenFilePath)
	}

	standardLogger := logrus.StandardLogger()
	if len(config.LogLevel) > 0 {
		level, _ := logrus.ParseLevel(config.LogLevel)
		standardLogger.SetLevel(level)
		loggers := log.NewLoggerWithLogrus(standardLogger)
		clientOptions.Logger = loggers
	}

	if config.TLSAllowInsecureConnection {
		clientOptions.TLSAllowInsecureConnection = config.TLSAllowInsecureConnection
	}
	producerOptions := pulsar.ProducerOptions{
		Topic:                   config.Topic,
		MaxPendingMessages:      config.MaxPendingMessages,
		CompressionType:         config.CompressionType,
		HashingScheme:           config.HashingScheme,
		BatchingMaxSize:         config.BatchingMaxSize,
		BatchingMaxPublishDelay: config.BatchingMaxPublishDelay,
		BatchingMaxMessages:     config.BatchingMaxMessages,
	}
	if len(config.Name) > 0 {
		producerOptions.Name = config.Name
	}
	if config.SendTimeout > 0 {
		producerOptions.SendTimeout = config.SendTimeout
	}
	if len(config.Properties) > 0 {
		producerOptions.Properties = config.Properties
	}
	return &clientOptions, &producerOptions, nil
}
