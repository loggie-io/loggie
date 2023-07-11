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
	"fmt"
	es "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	jsoniter "github.com/json-iterator/go"
	"github.com/loggie-io/loggie/pkg/core/api"
	eventer "github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/pkg/errors"
	"io/ioutil"
	"strconv"
	"strings"
)

var (
	json = jsoniter.ConfigFastest
)

type ClientSet struct {
	config *Config
	cli    *es.Client
	opType string

	buf      *bytes.Buffer
	aux      []byte
	reqCount int

	codec               codec.Codec
	indexPattern        *pattern.Pattern
	defaultIndexPattern *pattern.Pattern
	documentIdPattern   *pattern.Pattern
}

type Client interface {
	Bulk(ctx context.Context, batch api.Batch) error
	Stop()
}

func NewClient(config *Config, cod codec.Codec, indexPattern *pattern.Pattern, documentIdPattern *pattern.Pattern,
	defaultIndexPattern *pattern.Pattern) (*ClientSet, error) {
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

	return &ClientSet{
		config:              config,
		cli:                 cli,
		opType:              config.OpType,
		buf:                 bytes.NewBuffer(make([]byte, 0, config.SendBuffer)),
		aux:                 make([]byte, 0, 512),
		reqCount:            0,
		codec:               cod,
		indexPattern:        indexPattern,
		defaultIndexPattern: defaultIndexPattern,
		documentIdPattern:   documentIdPattern,
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
	defer func() {
		c.buf.Reset()
		c.reqCount = 0
	}()

	for _, event := range batch.Events() {
		headerObj := runtime.NewObject(event.Header())

		// select index
		idx, err := c.indexPattern.WithObject(headerObj).RenderWithStrict()
		if err != nil {
			failedConfig := c.config.IfRenderIndexFailed
			if !failedConfig.IgnoreError {
				log.Error("render elasticsearch index error: %v; event is: %s", err, event.String())
			}

			if failedConfig.DefaultIndex != "" { // if we had a default index, send events to this one
				defaultIdx, defaultIdxErr := c.defaultIndexPattern.WithObject(headerObj).Render()
				if defaultIdxErr != nil {
					log.Error("render default index error: %v", defaultIdxErr)
					continue
				}
				idx = defaultIdx
			} else if failedConfig.DropEvent {
				// ignore(drop) this event in default
				continue
			} else {
				return errors.WithMessage(err, "render elasticsearch index error")
			}
		}

		data, err := c.codec.Encode(event)
		if err != nil {
			return errors.WithMessagef(err, "codec encode event: %s error", event.String())
		}

		var docId string
		if c.config.DocumentId != "" {
			id, err := c.documentIdPattern.WithObject(headerObj).Render()
			if err != nil {
				return errors.WithMessagef(err, "format documentId %s failed", c.config.DocumentId)
			}
			docId = id
		}

		c.reqCount++
		if err := c.writeMeta(c.opType, docId, idx); err != nil {
			return err
		}
		if err := c.writeBody(data); err != nil {
			return err
		}
	}

	if c.reqCount == 0 {
		return errors.WithMessagef(eventer.ErrorDropEvent, "request to elasticsearch bulk is null")
	}

	resp, err := c.cli.Bulk(bytes.NewReader(c.buf.Bytes()),
		c.cli.Bulk.WithDocumentType(c.config.Etype),
		c.cli.Bulk.WithParameters(c.config.Params),
		c.cli.Bulk.WithHeader(c.config.Headers))
	if err != nil {
		return errors.WithMessagef(err, "request to elasticsearch bulk failed")
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}

	blkResp := BulkIndexerResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&blkResp); err != nil {
		out, _ := json.Marshal(resp.Body)
		return errors.Errorf("elasticsearch response error: %s", out)
	}

	if blkResp.HasErrors {
		failed := blkResp.Failed()
		failedCount := len(failed)
		// to avoid too many error messages
		if failedCount > 2 {
			failed = blkResp.Failed()[:1]
		}
		out, _ := json.Marshal(failed)

		// if there are some events succeed, retry will cause these events to be sent repeatedly, so here we will not retry
		if failedCount < len(blkResp.Items) {
			log.Error("partial bulk to elasticsearch response error, will drop failed events, all(%d), failed(%d), reason: %s", len(blkResp.Items), len(blkResp.Failed()), out)
			return nil
		}

		return errors.Errorf("all bulk to elasticsearch response error, all(%d), failed(%d), reason: %s", len(blkResp.Items), len(blkResp.Failed()), out)
	}

	return nil
}

func (c *ClientSet) Stop() {
	// Do nothing
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
	if len(body) == 0 {
		return nil
	}
	c.buf.Write(body)
	c.buf.WriteRune('\n')
	return nil
}
