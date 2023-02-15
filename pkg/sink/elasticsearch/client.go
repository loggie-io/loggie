/*
Copyright 2021 Loggie Authors

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

package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	eventer "github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"io/ioutil"
	"strconv"
	"strings"

	es "github.com/elastic/go-elasticsearch/v7"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"github.com/loggie-io/loggie/pkg/util/runtime"

	"github.com/pkg/errors"
)

type ClientSet struct {
	Version           string
	config            *Config
	cli               *es.Client
	buf               *bytes.Buffer
	aux               []byte
	opType            string
	codec             codec.Codec
	indexPattern      *pattern.Pattern
	documentIdPattern *pattern.Pattern
}

type Client interface {
	Bulk(ctx context.Context, batch api.Batch) error
	Stop()
}

type bulkResponse struct {
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			ID     string `json:"_id"`
			Result string `json:"result"`
			Status int    `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
				Cause  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"caused_by"`
			} `json:"error"`
		} `json:"index"`
	} `json:"items"`
}

func NewClient(config *Config, cod codec.Codec, indexPattern *pattern.Pattern, documentIdPattern *pattern.Pattern) (Client, error) {
	for i, h := range config.Hosts {
		if !strings.HasPrefix(h, "http") && !strings.HasPrefix(h, "https") {
			config.Hosts[i] = fmt.Sprintf("http://%s", h)
		}
	}

	var ca []byte
	if config.CACertPath != "" {
		caData, err := ioutil.ReadFile(config.CACertPath)
		if err != nil {
			return nil, err
		}
		ca = caData
	}

	cfg := es.Config{
		Addresses:           config.Hosts,
		DisableRetry:        true,
		Username:            config.UserName,
		Password:            config.Password,
		APIKey:              config.APIKey,
		ServiceToken:        config.ServiceToken,
		CompressRequestBody: config.Compress,
		CACert:              ca,
	}
	cli, err := es.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	reqBuf := bytes.NewBuffer(make([]byte, 0, config.SendBuffer))
	return &ClientSet{
		cli:               cli,
		config:            config,
		codec:             cod,
		indexPattern:      indexPattern,
		documentIdPattern: documentIdPattern,
		buf:               reqBuf,
		aux:               make([]byte, 0, 512),
		opType:            config.OpType,
	}, nil
}

func (c *ClientSet) Bulk(ctx context.Context, batch api.Batch) error {
	if len(batch.Events()) == 0 {
		return errors.WithMessagef(eventer.ErrorDropEvent, "request to elasticsearch bulk is null")
	}

	bulkReq := esapi.BulkRequest{}
	if c.config.Etype != "" {
		bulkReq.DocumentType = c.config.Etype
	}
	defer c.buf.Reset()

	for _, event := range batch.Events() {
		headerObj := runtime.NewObject(event.Header())

		// select index
		idx, err := c.indexPattern.WithObject(headerObj).RenderWithStrict()
		if err != nil {
			log.Error("render index pattern err: %v; event is: %s", err, event.String())
			continue
		}

		data, err := c.codec.Encode(event)
		if err != nil {
			return errors.WithMessagef(err, "codec encode event: %s error", event.String())
		}

		docId := ""
		if c.config.DocumentId != "" {
			id, err := c.documentIdPattern.WithObject(headerObj).Render()
			if err != nil {
				return errors.WithMessagef(err, "format documentId %s failed", c.config.DocumentId)
			}
			docId = id
		}

		if err := c.writeMeta(c.opType, docId, idx); err != nil {
			return err
		}
		if err := c.writeBody(data); err != nil {
			return err
		}
	}

	resp, err := c.cli.Bulk(bytes.NewReader(c.buf.Bytes()), c.cli.Bulk.WithDocumentType(c.config.Etype))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		blkResp := bulkResponse{}
		err := json.NewDecoder(resp.Body).Decode(&blkResp)
		if err != nil {
			out, _ := json.Marshal(resp.Body)
			return errors.Errorf("elasticsearch response error: %s", out)
		}

		// get more error details
		for _, d := range blkResp.Items {
			if d.Index.Status > 201 {
				log.Error("elasticsearch response error, item: [%d]: %s: %s: %s: %s",
					d.Index.Status,
					d.Index.Error.Type,
					d.Index.Error.Reason,
					d.Index.Error.Cause.Type,
					d.Index.Error.Cause.Reason,
				)
			}
		}
		return errors.New("elasticsearch response error")
	}

	return nil
}

func (c *ClientSet) Stop() {
}

// { "index" : { "_index" : "test", "_id" : "1" } }
func (c *ClientSet) writeMeta(action string, documentID string, index string) error {
	c.buf.WriteRune('{')
	c.aux = strconv.AppendQuote(c.aux, action)
	c.buf.Write(c.aux)
	c.aux = c.aux[:0]
	c.buf.WriteRune(':')
	c.buf.WriteRune('{')
	if documentID != "" {
		c.buf.WriteString(`"_id":`)
		c.aux = strconv.AppendQuote(c.aux, documentID)
		c.buf.Write(c.aux)
		c.aux = c.aux[:0]
	}

	if index != "" {
		c.buf.WriteString(`"_index":`)
		c.aux = strconv.AppendQuote(c.aux, index)
		c.buf.Write(c.aux)
		c.aux = c.aux[:0]
	}
	c.buf.WriteRune('}')
	c.buf.WriteRune('}')
	c.buf.WriteRune('\n')
	return nil
}

func (c *ClientSet) writeBody(body []byte) error {
	c.buf.Write(body)
	c.buf.WriteRune('\n')
	return nil
}
