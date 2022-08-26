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
	"context"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/log"
	"strings"
	"time"

	es "github.com/olivere/elastic/v7"
)

type ClientSet struct {
	config *Config
	cli    *es.Client
	db     *dbHandler
	offset string
	done   chan struct{}
}

func NewClient(config *Config) (*ClientSet, error) {
	for i, h := range config.Hosts {
		if !strings.HasPrefix(h, "http") && !strings.HasPrefix(h, "https") {
			config.Hosts[i] = fmt.Sprintf("http://%s", h)
		}
	}
	var opts []es.ClientOptionFunc
	opts = append(opts, es.SetURL(config.Hosts...))
	if config.Sniff != nil {
		opts = append(opts, es.SetSniff(*config.Sniff))
	} else {
		// disable sniff by default
		opts = append(opts, es.SetSniff(false))
	}
	if config.Password != "" && config.UserName != "" {
		opts = append(opts, es.SetBasicAuth(config.UserName, config.Password))
	}
	if config.Schema != "" {
		opts = append(opts, es.SetScheme(config.Schema))
	}
	if config.Gzip != nil {
		opts = append(opts, es.SetGzip(*config.Gzip))
	}

	cli, err := es.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	c := &ClientSet{
		cli:    cli,
		config: config,
	}
	go c.watch()
	return c, nil
}

func (c *ClientSet) watch() {
	flushT := time.NewTicker(c.config.DbConfig.FlushTimeout)
	defer flushT.Stop()

	cleanT := time.NewTicker(c.config.DbConfig.CleanScanInterval)
	defer cleanT.Stop()

	for {
		select {
		case <-c.done:
			return

		case <-flushT.C:
			c.persistentOffset()
		case <-cleanT.C:
			c.cleanInactiveRecord()
		}
	}
}

func (c *ClientSet) persistentOffset() {
	record := c.db.findBy(c.config.Name, c.config.PipelineName)
	if record.SourceName == "" {
		record.PipelineName = c.config.PipelineName
		record.SourceName = c.config.Name
		record.Source = strings.Join(c.config.Hosts, ",")
	}
	record.Offset = c.offset
	c.db.upsertRecord(record)
}

func (c *ClientSet) cleanInactiveRecord() {
	if c.config.DbConfig.CleanInactiveTimeout <= 0 {
		return
	}
	record := c.db.findBy(c.config.Name, c.config.PipelineName)
	if record.SourceName == "" {
		return
	}
	collectTime, err := time.Parse("2006-01-02 15:04", record.CollectTime)
	if err != nil {
		log.Warn("[%s]parse time of collectTime error: %v", c.config.PipelineName, err)
		return
	}

	if time.Since(collectTime) < c.config.DbConfig.CleanInactiveTimeout {
		return
	}

	c.db.delete(record)
}

func (c *ClientSet) Search(ctx context.Context) ([][]byte, error) {
	queryBuilder := c.cli.Search().Index(c.config.Index)
	if c.config.Query != "" {
		queryBuilder.Query(es.NewRawStringQuery(c.config.Query))
	}
	if len(c.config.IncludeFields) > 0 || len(c.config.ExcludeFields) > 0 {
		fsc := es.NewFetchSourceContext(true)
		if len(c.config.IncludeFields) > 0 {
			fsc.Include(c.config.IncludeFields...)
		}
		if len(c.config.ExcludeFields) > 0 {
			fsc.Exclude(c.config.ExcludeFields...)
		}
		queryBuilder.FetchSourceContext(fsc)
	}

	if c.offset == "" {
		record := c.db.findBy(c.config.Name, c.config.PipelineName)
		if record.SourceName != "" {
			c.offset = record.Offset
		}
	}

	if c.offset != "" {
		s := strings.Split(c.offset, ",")
		offset := make([]interface{}, 0, len(s))
		for i := 0; i < len(s); i++ {
			offset = append(offset, s[i])
		}
		queryBuilder.SearchAfter(offset...)
	}

	result, err := queryBuilder.Sort("_id", true).Sort("_score", false).Size(c.config.Size).Do(ctx)
	if err != nil {
		return nil, err
	}
	datas := make([][]byte, 0)
	if len(result.Hits.Hits) > 0 {
		var lastSort []interface{}
		for i := 0; i < len(result.Hits.Hits); i++ {
			v := result.Hits.Hits[i]
			lastSort = v.Sort
			bt, err := v.Source.MarshalJSON()
			if err != nil {
				return nil, err
			}
			datas = append(datas, bt)
		}
		// record offset
		offset := make([]string, 0, len(lastSort))
		for i := 0; i < len(lastSort); i++ {
			offset = append(offset, fmt.Sprintf("%v", lastSort[i]))
		}
		c.offset = strings.Join(offset, ",")
	}
	return datas, nil
}

func (c *ClientSet) Stop() {
	if c.cli != nil {
		c.cli.Stop()
	}
	c.persistentOffset()
	close(c.done)
}
