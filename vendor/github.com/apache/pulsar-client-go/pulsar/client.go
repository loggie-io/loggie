// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"crypto/tls"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal/auth"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

// NewClient Creates a pulsar client instance
func NewClient(options ClientOptions) (Client, error) {
	return newClient(options)
}

// Authentication Opaque interface that represents the authentication credentials
type Authentication interface{}

// NewAuthentication Creates an authentication by name and params
func NewAuthentication(name string, params string) (Authentication, error) {
	return auth.NewProvider(name, params)
}

// NewAuthenticationToken Creates new Authentication provider with specified auth token
func NewAuthenticationToken(token string) Authentication {
	return auth.NewAuthenticationToken(token)
}

// NewAuthenticationTokenFromSupplier returns a token auth provider that
// gets the token data from a user supplied function. The function is
// invoked each time the client library needs to use a token in talking
// with Pulsar brokers
func NewAuthenticationTokenFromSupplier(tokenSupplier func() (string, error)) Authentication {
	return auth.NewAuthenticationTokenFromSupplier(tokenSupplier)
}

// NewAuthenticationTokenFromFile Creates new Authentication provider with specified auth token from a file
func NewAuthenticationTokenFromFile(tokenFilePath string) Authentication {
	return auth.NewAuthenticationTokenFromFile(tokenFilePath)
}

// NewAuthenticationTLS Creates new Authentication provider with specified TLS certificate and private key
func NewAuthenticationTLS(certificatePath string, privateKeyPath string) Authentication {
	return auth.NewAuthenticationTLS(certificatePath, privateKeyPath)
}

// NewAuthenticationFromTLSCertSupplier Create new Authentication provider with specified TLS certificate supplier
func NewAuthenticationFromTLSCertSupplier(tlsCertSupplier func() (*tls.Certificate, error)) Authentication {
	return auth.NewAuthenticationFromTLSCertSupplier(tlsCertSupplier)
}

// NewAuthenticationAthenz Creates Athenz Authentication provider
func NewAuthenticationAthenz(authParams map[string]string) Authentication {
	athenz, _ := auth.NewAuthenticationAthenzWithParams(authParams)
	return athenz
}

// NewAuthenticationOAuth2 Creates OAuth2 Authentication provider
func NewAuthenticationOAuth2(authParams map[string]string) Authentication {
	oauth, _ := auth.NewAuthenticationOAuth2WithParams(authParams)
	return oauth
}

// ClientOptions is used to construct a Pulsar Client instance.
type ClientOptions struct {
	// Configure the service URL for the Pulsar service.
	// This parameter is required
	URL string

	// Timeout for the establishment of a TCP connection (default: 5 seconds)
	ConnectionTimeout time.Duration

	// Set the operation timeout (default: 30 seconds)
	// Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
	// operation will be marked as failed
	OperationTimeout time.Duration

	// Configure the authentication provider. (default: no authentication)
	// Example: `Authentication: NewAuthenticationTLS("my-cert.pem", "my-key.pem")`
	Authentication

	// Set the path to the trusted TLS certificate file
	TLSTrustCertsFilePath string

	// Configure whether the Pulsar client accept untrusted TLS certificate from broker (default: false)
	TLSAllowInsecureConnection bool

	// Configure whether the Pulsar client verify the validity of the host name from broker (default: false)
	TLSValidateHostname bool

	// Configure the net model for vpc user to connect the pulsar broker
	ListenerName string

	// Max number of connections to a single broker that will kept in the pool. (Default: 1 connection)
	MaxConnectionsPerBroker int

	// Configure the logger used by the client.
	// By default, a wrapped logrus.StandardLogger will be used, namely,
	// log.NewLoggerWithLogrus(logrus.StandardLogger())
	// FIXME: use `logger` as internal field name instead of `log` as it's more idiomatic
	Logger log.Logger

	// Specify metric cardinality to the tenant, namespace or topic levels, or remove it completely.
	// Default: MetricsCardinalityNamespace
	MetricsCardinality MetricsCardinality

	// Add custom labels to all the metrics reported by this client instance
	CustomMetricsLabels map[string]string
}

// Client represents a pulsar client
type Client interface {
	// CreateProducer Creates the producer instance
	// This method will block until the producer is created successfully
	CreateProducer(ProducerOptions) (Producer, error)

	// Subscribe Creates a `Consumer` by subscribing to a topic.
	//
	// If the subscription does not exist, a new subscription will be created and all messages published after the
	// creation will be retained until acknowledged, even if the consumer is not connected
	Subscribe(ConsumerOptions) (Consumer, error)

	// CreateReader Creates a Reader instance.
	// This method will block until the reader is created successfully.
	CreateReader(ReaderOptions) (Reader, error)

	// TopicPartitions Fetches the list of partitions for a given topic
	//
	// If the topic is partitioned, this will return a list of partition names.
	// If the topic is not partitioned, the returned list will contain the topic
	// name itself.
	//
	// This can be used to discover the partitions and create {@link Reader},
	// {@link Consumer} or {@link Producer} instances directly on a particular partition.
	TopicPartitions(topic string) ([]string, error)

	// Close Closes the Client and free associated resources
	Close()
}

// MetricsCardinality represents the specificty of labels on a per-metric basis
type MetricsCardinality int

const (
	_                           MetricsCardinality = iota
	MetricsCardinalityNone                         // Do not add additional labels to metrics
	MetricsCardinalityTenant                       // Label metrics by tenant
	MetricsCardinalityNamespace                    // Label metrics by tenant and namespace
	MetricsCardinalityTopic                        // Label metrics by topic
)
