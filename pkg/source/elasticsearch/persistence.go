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
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"time"
)

type DBConfig struct {
	IndexPrefix          string        `yaml:"indexPrefix,omitempty" default:".loggie-db"`
	FlushTimeout         time.Duration `yaml:"flushTimeout,omitempty" default:"2s"`
	CleanInactiveTimeout time.Duration `yaml:"cleanInactiveTimeout,omitempty" default:"504h"` // default records not updated in 21 days will be deleted
	CleanScanInterval    time.Duration `yaml:"cleanScanInterval,omitempty" default:"1h"`
}

type Offset struct {
	Sort      []interface{}          `json:"-"`
	Body      map[string]interface{} `json:"sorts"`
	CreatedAt time.Time              `json:"created_at"`
}

func (o *Offset) makeBody(keys []string) {
	if o.Body == nil {
		o.Body = make(map[string]interface{})
	}
	for i, k := range keys {
		o.Body[k] = o.Sort[i]
	}
}

type DB struct {
	index string
	es    *elastic.Client
}

func NewDB(index string, es *elastic.Client) *DB {
	return &DB{index: index, es: es}
}

func (p *DB) Search(ctx context.Context) (*Offset, error) {
	exists, err := p.es.IndexExists(p.index).Do(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	result, err := p.es.Search().
		Index(p.index).
		From(0).Size(1).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	if len(result.Hits.Hits) == 0 {
		return nil, nil
	}

	ost := new(Offset)
	v := result.Hits.Hits[0]
	bt, err := v.Source.MarshalJSON()
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(bt, ost); err != nil {
		return nil, err
	}
	return ost, nil
}

func (p *DB) Upsert(ctx context.Context, ost *Offset, sortKeys []string) error {
	ost.makeBody(sortKeys)
	body, err := json.Marshal(ost)
	if err != nil {
		return errors.WithMessagef(err, "json marshal db offset")
	}

	if _, err := p.es.Index().Index(p.index).Id("1").BodyString(string(body)).
		Do(ctx); err != nil {
		return err
	}

	return nil
}

func (p *DB) Remove(ctx context.Context) (err error) {
	_, err = p.es.DeleteIndex(p.index).Do(ctx)
	return
}
