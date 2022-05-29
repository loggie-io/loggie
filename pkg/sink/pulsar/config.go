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
	OperationTimeoutSeconds    time.Duration `yaml:"operation_timeout_seconds,omitempty"`
	UseTLS                     bool          `yaml:"useTLS,omitempty"`
	TLSTrustCertsFilePath      string        `yaml:"tlsTrustCertsFilePath,omitempty"`
	TLSAllowInsecureConnection bool          `yaml:"tlsAllowInsecureConnection,omitempty"`
	// Max number of connections to a single broker that is kept in the pool. default 1
	MaxConnectionsPerBroker int    `yaml:"maxConnectionPerBroker,omitempty"`
	LogLevel                string `yaml:"logLevel,omitempty"`

	CertificatePath string `yaml:"certificatePath,omitempty"`
	PrivateKeyPath  string `yaml:"privateKeyPath,omitempty"`
	Token           string `yaml:"token,omitempty"`
	TokenFilePath   string `yaml:"tokenFilePath,omitempty"`
	// Timeout for the establishment of a TCP connection
	ConnectionTimeout time.Duration `yaml:"connectionTimeout,omitempty"`

	// producer name
	Name       string            `yaml:"name,omitempty"`
	Properties map[string]string `yaml:"properties,omitempty"`
	// SendTimeout set the timeout for a message that is not acknowledged by the server 30s.default 30s
	SendTimeout time.Duration `yaml:"sendTimeout,omitempty"`
	// MaxPendingMessages set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.
	MaxPendingMessages      int                    `yaml:"maxPendingMessages,omitempty"`
	HashingScheme           pulsar.HashingScheme   `yaml:"hashingSchema,omitempty"`
	CompressionType         pulsar.CompressionType `yaml:"compressionType,omitempty"`
	Batching                bool                   `yaml:"batching,omitempty"`
	BatchingMaxPublishDelay time.Duration          `yaml:"batchingMaxPublishDelay,omitempty"`
	BatchingMaxMessages     uint                   `yaml:"batchingMaxMessages,omitempty"`
	BatchingMaxSize         uint                   `yaml:"batchingMaxSize,omitempty"`
}

func (c *pulsarConfig) Validate() error {
	if len(c.URL) == 0 {
		return errors.New("no URL configured")
	}
	if len(c.Topic) == 0 {
		return errors.New("no topic configured")
	}
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
	if len(c.LogLevel) > 0 {
		_, err := logrus.ParseLevel(c.LogLevel)
		if err != nil {
			return err
		}
	}
	if c.CompressionType < 0 {
		return errors.New("compression_type is incorrect")
	}
	return nil
}

func getOptions(
	config *pulsarConfig,
) (*pulsar.ClientOptions, *pulsar.ProducerOptions, error) {
	err := config.Validate()
	if err != nil {
		return nil, nil, err
	}
	clientOptions := pulsar.ClientOptions{
		URL: config.URL,
	}
	if config.UseTLS {
		clientOptions.TLSTrustCertsFilePath = config.TLSTrustCertsFilePath
		if len(config.CertificatePath) > 0 {
			clientOptions.Authentication = pulsar.NewAuthenticationTLS(config.CertificatePath, config.PrivateKeyPath)
		}
	}
	if len(config.Token) > 0 {
		clientOptions.Authentication = pulsar.NewAuthenticationToken(string(config.Token))
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

	if config.OperationTimeoutSeconds > 0 {
		clientOptions.OperationTimeout = config.OperationTimeoutSeconds
	}
	if config.ConnectionTimeout > 0 {
		clientOptions.ConnectionTimeout = config.ConnectionTimeout
	}

	if config.TLSAllowInsecureConnection {
		clientOptions.TLSAllowInsecureConnection = config.TLSAllowInsecureConnection
	}
	producerOptions := pulsar.ProducerOptions{
		Topic: config.Topic,
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
	if config.MaxPendingMessages > 0 {
		producerOptions.MaxPendingMessages = config.MaxPendingMessages
	}

	if config.HashingScheme > 0 {
		producerOptions.HashingScheme = config.HashingScheme
	}
	if config.CompressionType > 0 {
		producerOptions.CompressionType = config.CompressionType
	}
	if config.BatchingMaxSize > 0 {
		producerOptions.BatchingMaxSize = config.BatchingMaxSize
	}
	if config.BatchingMaxPublishDelay > 0 {
		producerOptions.BatchingMaxPublishDelay = config.BatchingMaxPublishDelay
	}
	if config.BatchingMaxMessages > 0 {
		producerOptions.BatchingMaxMessages = config.BatchingMaxMessages
	}
	return &clientOptions, &producerOptions, nil
}
