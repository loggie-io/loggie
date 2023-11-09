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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
)

const Type = "clickhouse"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Sink struct {
	config   *Config
	conn     driver.Conn
	colNames []string
	cod      codec.Codec
}

func NewSink() *Sink {
	return &Sink{
		config: &Config{},
	}
}

func (s *Sink) Config() interface{} {
	return s.config
}

func (s *Sink) SetCodec(c codec.Codec) {
	s.cod = c
}

func (s *Sink) Category() api.Category {
	return api.SINK
}

func (s *Sink) Type() api.Type {
	return Type
}

func (s *Sink) String() string {
	return fmt.Sprintf("%s/%s", api.SINK, Type)
}

func (s *Sink) Init(context api.Context) error {
	if len(s.config.HttpHeaders) > 0 {
		var headers = make(map[string]string)
		for _, colums := range s.config.HttpHeaders {
			var k, v string
			splited := strings.Split(colums, ",")
			if len(splited) >= 2 {
				k, v = splited[0], splited[1]
			} else {
				k, v = splited[0], ""
			}

			headers[k] = v
		}
		s.config.Headers = headers
	}

	return nil
}

func (s *Sink) Start() (err error) {
	opt := &clickhouse.Options{
		Protocol: *s.config.Protocol,
		TLS:      s.config.TLS,
		Addr:     s.config.Addr,
		Auth: clickhouse.Auth{
			Database: s.config.Database,
			Username: s.config.Username,
			Password: s.config.Password,
		},
		ReadTimeout:     s.config.ReadTimeout,
		DialTimeout:     s.config.DialTimeout,
		MaxOpenConns:    s.config.MaxOpenConns,
		MaxIdleConns:    s.config.MaxIdleConns,
		ConnMaxLifetime: s.config.ConnMaxLifetime,
		HttpHeaders:     s.config.Headers,
		Debug:           s.config.Debug,
		Debugf:          s.config.Debugf,
		BlockBufferSize: s.config.BlockBufferSize,
	}

	if s.config.Compression != nil {
		opt.Compression = &clickhouse.Compression{
			Method: s.config.Compression.Method,
			Level:  s.config.Compression.Level,
		}
		opt.MaxCompressionBuffer = s.config.Compression.MaxBuffer
	}

	if s.config.ConnOpenStrategy != nil {
		opt.ConnOpenStrategy = *s.config.ConnOpenStrategy
	}

	s.conn, err = clickhouse.Open(opt)
	if err != nil {
		return
	}

	s.colNames, err = s.getColums(context.Background())
	return
}

func (s *Sink) Stop() {
	if s.conn != nil {
		_ = s.conn.Close()
	}
}

func (s *Sink) Consume(batch api.Batch) api.Result {
	log.Debug("start to consume events")
	insertQ := fmt.Sprintf("INSERT INTO `%s`.`%s`", s.config.Database, s.config.Table)
	driverBatch, err := s.conn.PrepareBatch(context.Background(), insertQ)
	if err != nil {
		return result.Fail(err)
	}

	events := batch.Events()
	l := len(events)
	if l == 0 {
		return nil
	}

	var rows [][]interface{}
	if err := s.getRowsFromEvents(events, &rows); err != nil {
		return result.Fail(err)
	}

	if len(rows) == 0 {
		return result.DropWith(errors.New("send to clickhouse message batch is null"))
	}

	for i := range rows {
		if err := driverBatch.Append(rows[i]...); err != nil {
			log.Warn("append to clickhouse batch failed: %s", err.Error())
			return result.Fail(err)
		}
	}

	if err := driverBatch.Send(); err != nil {
		log.Warn("send to clickhouse failed: %s", err.Error())
		return result.Fail(err)
	} else {
		log.Info("events %d, send rows %d to clickhouse success", len(events), len(rows))
		return result.Success()
	}
}

func (s *Sink) getColums(ctx context.Context) (colNames []string, err error) {
	queryTableSchema := fmt.Sprintf("DESCRIBE TABLE `%s`.`%s`", s.config.Database, s.config.Table)
	r, err := s.conn.Query(ctx, queryTableSchema)
	if err != nil {
		return
	}

	for r.Next() {
		var (
			colName string
			colType string
			ignore  string
		)

		if err = r.Scan(&colName, &colType, &ignore, &ignore, &ignore, &ignore, &ignore); err != nil {
			return nil, err
		}
		colNames = append(colNames, colName)
	}

	return
}

func (s *Sink) getRowsFromEvents(events []api.Event, rows *[][]interface{}) error {
	for _, e := range events {
		data, err := s.cod.Encode(e)
		if err != nil {
			log.Warn("encode event error: %+v", err)
			return err
		}

		var (
			rowMap map[string]interface{}
			row    []interface{}
		)

		err = json.Unmarshal(data, &rowMap)
		if err != nil {
			log.Warn("unmarshal event error: %+v", err)
			return err
		}

		var skipRow bool
		for i := 0; i < len(s.colNames); i++ {
			colName := s.colNames[i]
			val, ok := rowMap[colName]
			if ok {
				row = append(row, val)
				continue
			}

			if s.config.SkipRowIfFieldNull {
				log.Warn("field %s val not found, would skip this row", colName)
				skipRow = true
				break
			}

			log.Warn("field %s val not found, would append null", colName)
			row = append(row, nil)
		}

		if skipRow {
			continue
		}

		*rows = append(*rows, row)
	}

	return nil
}
