/*
Copyright 2023 Loggie Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package clickhouse

import (
	"crypto/tls"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/loggie-io/loggie/pkg/core/log"
)

type Compression struct {
	MaxBuffer int `yaml:"maxBuffer,omitempty"` // default 10485760 - measured in bytes  i.e. 10MiB

	Method clickhouse.CompressionMethod `yaml:"method,omitempty"`
	Level  int                          `yaml:"level,omitempty"`
}

type Config struct {
	// protocol related
	Protocol         *clickhouse.Protocol         `yaml:"protocol,omitempty"`
	TLS              *tls.Config                  `yaml:"tls,omitempty"`
	Addr             []string                     `yaml:"addr,omitempty" validate:"required"`
	ReadTimeout      time.Duration                `yaml:"readTimeout,omitempty"`     // default 5 second
	DialTimeout      time.Duration                `yaml:"dialTimeout,omitempty"`     // default 30 second
	MaxOpenConns     int                          `yaml:"maxOpenConns,omitempty"`    // default MaxIdleConns + 5
	MaxIdleConns     int                          `yaml:"maxIdleConns,omitempty"`    // default 5
	ConnMaxLifetime  time.Duration                `yaml:"connMaxLifetime,omitempty"` // default 1 hour
	ConnOpenStrategy *clickhouse.ConnOpenStrategy `yaml:"connOpenStrategy,omitempty"`
	HttpHeaders      []string                     `yaml:"headers,omitempty"`  // set additional headers on HTTP requests
	Headers          map[string]string            `yaml:"-"`                  // container for headers
	HttpUrlPath      string                       `yaml:"urrlPath,omitempty"` // set additional URL path for HTTP requests

	// db related
	Database string `yaml:"database,omitempty" validate:"required"`
	Table    string `yaml:"table,omitempty" validate:"required"`
	Username string `yaml:"user,omitempty" validate:"required"`
	Password string `yaml:"password,omitempty" validate:"required"`

	// debug related
	Debug  bool                          `yaml:"debug,omitempty"`
	Debugf func(format string, v ...any) // only works when Debug is true

	// compression related
	*Compression `yaml:"compression,omitempty"`

	// buffer related
	BlockBufferSize uint8 `yaml:"blockBufferSize,omitempty"` // default 2 - can be overwritten on query

	//skip null row if enabled, by default would append nil
	SkipRowIfFieldNull bool `yaml:"skipRowIfFieldNull,omitempty"`
}

func (c *Config) SetDefaults() {
	if c.Protocol == nil {
		protocol := clickhouse.Native
		c.Protocol = &protocol
	}

	if c.Addr == nil {
		c.Addr = []string{"localhost:8123"}
	}

	if c.Debugf == nil {
		c.Debugf = func(format string, v ...any) {
			log.Debug(format, v...)
		}
	}

	if c.ReadTimeout <= 0 {
		c.ReadTimeout = 300 * time.Second
	}

	if c.DialTimeout <= 0 {
		c.DialTimeout = 30 * time.Second
	}

	if c.MaxOpenConns <= 0 {
		c.MaxOpenConns = 256
	}

	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = 32
	}

	if c.ConnMaxLifetime <= 0 {
		c.ConnMaxLifetime = 1 * time.Minute
	}
}

func (c *Config) Validate() error {
	if *c.Protocol == clickhouse.Native && c.Compression != nil {
		return errors.New("native protocol not support compression")
	}

	if c.Database == "" {
		return errors.New("database required")
	}

	if c.Table == "" {
		return errors.New("table required")
	}

	if c.Username == "" {
		return errors.New("user required")
	}

	if c.Password == "" {
		return errors.New("password required")
	}

	return nil
}
