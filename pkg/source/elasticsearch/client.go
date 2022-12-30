/*
Copyright 2022 Loggie Authors

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
	db     *DB
	offset *Offset
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
		db:     NewDB(fmt.Sprintf("%s-%s-%s", config.DBConfig.IndexPrefix, config.PipelineName, config.Name), cli),
	}
	go c.watch()
	return c, nil
}

func (c *ClientSet) watch() {
	flushT := time.NewTicker(c.config.DBConfig.FlushTimeout)
	defer flushT.Stop()

	cleanT := time.NewTicker(c.config.DBConfig.CleanScanInterval)
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
	if c.offset == nil {
		return
	}
	if err := c.db.Upsert(context.Background(), c.offset); err != nil {
		log.Warn("[%s-%s]persistent offset error: %v", c.config.PipelineName, c.config.Name, err)
	}
}

func (c *ClientSet) cleanInactiveRecord() {
	ctx := context.Background()
	if c.config.DBConfig.CleanInactiveTimeout <= 0 {
		return
	}
	record, err := c.db.Search(ctx)
	if err != nil {
		log.Warn("[%s-%s]search offset error: %v", c.config.PipelineName, c.config.Name, err)
		return
	}
	if record == nil {
		return
	}

	if time.Since(record.CreatedAt) < c.config.DBConfig.CleanInactiveTimeout {
		return
	}

	if err := c.db.Remove(ctx); err != nil {
		log.Warn("[%s-%s]remove offset error: %v", c.config.PipelineName, c.config.Name, err)
	}
}

func (c *ClientSet) Search(ctx context.Context) ([][]byte, error) {
	queryBuilder := c.cli.Search().Index(c.config.Indices...)
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

	if c.offset == nil {
		ost, err := c.db.Search(ctx)
		if err != nil {
			return nil, err
		}
		c.offset = ost
	}

	if c.offset != nil {
		queryBuilder.SearchAfter(c.offset.Uid, c.offset.Score)
	}

	result, err := queryBuilder.Sort("_id", true).Sort("_score", false).Size(c.config.Size).Do(ctx)
	if err != nil {
		return nil, err
	}
	datas := make([][]byte, 0)
	if len(result.Hits.Hits) > 0 {
		var last *es.SearchHit
		for i := 0; i < len(result.Hits.Hits); i++ {
			v := result.Hits.Hits[i]
			bt, err := v.Source.MarshalJSON()
			if err != nil {
				return nil, err
			}
			datas = append(datas, bt)
			last = v
		}
		// record offset
		c.offset = &Offset{
			Uid:       last.Sort[0],
			Score:     last.Sort[1],
			CreatedAt: time.Now(),
		}
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
