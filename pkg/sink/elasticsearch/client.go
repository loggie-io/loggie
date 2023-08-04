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
	jsoniter "github.com/json-iterator/go"
	"github.com/loggie-io/loggie/pkg/core/api"
	eventer "github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/pkg/errors"
	"os"
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

	reqCount int

	codec               codec.Codec
	indexPattern        *pattern.Pattern
	defaultIndexPattern *pattern.Pattern
	documentIdPattern   *pattern.Pattern
}

type bulkRequest struct {
	lines []line
}

type line struct {
	meta []byte
	body []byte
}

func (b *bulkRequest) body() []byte {
	var buf bytes.Buffer
	size := 0
	for _, l := range b.lines {
		size += len(l.meta) + len(l.body) + 1
	}
	buf.Grow(size)

	for _, l := range b.lines {
		buf.Write(l.meta)
		buf.Write(l.body)
		buf.WriteRune('\n')
	}
	return buf.Bytes()
}

func (b *bulkRequest) add(body []byte, action string, documentID string, index string) {
	if len(body) == 0 {
		return
	}

	var buf bytes.Buffer
	var aux []byte

	// { "index" : { "_index" : "test", "_id" : "1" } }
	buf.WriteRune('{')
	aux = strconv.AppendQuote(aux, action)
	buf.Write(aux)
	aux = aux[:0]
	buf.WriteRune(':')
	buf.WriteRune('{')
	if documentID != "" {
		buf.WriteString(`"_id":`)
		aux = strconv.AppendQuote(aux, documentID)
		buf.Write(aux)
		aux = aux[:0]
	}

	if index != "" {
		buf.WriteString(`"_index":`)
		aux = strconv.AppendQuote(aux, index)
		buf.Write(aux)
	}
	buf.WriteRune('}')
	buf.WriteRune('}')
	buf.WriteRune('\n')

	l := line{
		meta: buf.Bytes(),
		body: body,
	}

	b.lines = append(b.lines, l)
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
		caData, err := os.ReadFile(config.CACertPath)
		if err != nil {
			return nil, err
		}
		ca = caData
	}

	cfg := es.Config{
		Addresses:             config.Hosts,
		DisableRetry:          true,
		Username:              config.UserName,
		Password:              config.Password,
		APIKey:                config.APIKey,
		ServiceToken:          config.ServiceToken,
		CompressRequestBody:   config.Compress,
		DiscoverNodesOnStart:  config.DiscoverNodesOnStart,
		DiscoverNodesInterval: config.DiscoverNodesInterval,
		CACert:                ca,
	}
	cli, err := es.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return &ClientSet{
		config:              config,
		cli:                 cli,
		opType:              config.OpType,
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

	defer func() {
		c.reqCount = 0
	}()

	req := bulkRequest{}
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
		req.add(data, c.opType, docId, idx)
	}

	if c.reqCount == 0 {
		return errors.WithMessagef(eventer.ErrorDropEvent, "request to elasticsearch bulk is null")
	}

	resp, err := c.cli.Bulk(bytes.NewReader(req.body()),
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
