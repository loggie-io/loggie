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

package aggregator

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/util/eventops"
	"strings"
	"time"
)

const Type = "aggregator"

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	return &aggregator{
		stop:      make(chan struct{}),
		inChan: make(chan []api.Event),
		groupValue: make(map[string]Selected),
		config: &Config{},
		eventPool: info.EventPool,
	}
}

type aggregator struct {
	pipelineName string
	name      string
	stop      chan struct{}

	inChan chan []api.Event
	groupValue map[string]Selected // key: groupBy

	config *Config
	eventPool *event.Pool
}

type Selected struct {
	groupBy []groupByKeyVal
	val map[string]Operator // key: select.key
}

type groupByKeyVal struct {
	key string
	val string
}


func (d *aggregator) Config() interface{} {
	return d.config
}

func (d *aggregator) Category() api.Category {
	return api.SOURCE
}

func (d *aggregator) Type() api.Type {
	return Type
}

func (d *aggregator) String() string {
	return fmt.Sprintf("%s/%s", api.SOURCE, Type)
}

func (d *aggregator) Init(context api.Context) error {
	d.name = context.Name()
	return nil
}

func (d *aggregator) Start() error {
	return nil
}

func (d *aggregator) Stop() {
	close(d.stop)
}

func (d *aggregator) In(events []api.Event) {
	d.inChan <- events
}

func (d *aggregator) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", d.String())
	tick := time.NewTicker(d.config.Interval)
	defer tick.Stop()
	for {
		select {
		case <-d.stop:
			return

		case <-tick.C:
			// generate new events after aggregate
			d.export(productFunc)

		case events := <-d.inChan:
			// aggregate events
			log.Info("---> events: %d", len(events))
			d.aggregate(events)
		}
	}
}

func (d *aggregator) Commit(events []api.Event) {
	d.eventPool.PutAll(events)
}

func (d *aggregator) SetSourceConfig(config *source.Config, pipelineName string) {
	d.pipelineName = pipelineName
}

func (d *aggregator) aggregate(events []api.Event) {
	for _, e := range events {
		// generate groupBy identity
		id, groupKeyVal := groupIdentity(d.config.GroupBy, e)

		// 检测是否有该id对应的数据
		selected, ok := d.groupValue[id]
		if !ok {
			val := initOperatorValue(d.config.Select, e)
			d.groupValue[id] = Selected{
				groupBy: groupKeyVal,
				val: val,
			}
			continue
		}

		// 根据不同的operator计算和聚合
		for _, s := range d.config.Select {
			v, ok := selected.val[s.Key]
			if !ok {
				continue
			}

			err := v.compute(e, s.Key)
			if err != nil {
				log.Error("%+v", err)
				continue
			}
		}

	}
}

func groupIdentity(groupBy []string, e api.Event) (string, []groupByKeyVal) {
	var groupByVal []groupByKeyVal
	var sb strings.Builder
	for _, g := range groupBy {
		r := eventops.GetString(e, g) // TODO what if fields is a number?
		if r == "" {
			continue
		}
		sb.WriteString(r)
		sb.WriteString("_")

		groupByVal = append(groupByVal, groupByKeyVal{key: g, val: r})
	}

	return sb.String(), groupByVal
}

func (d *aggregator) export(productFunc api.ProductFunc) {
	for _, gp := range d.groupValue {
		header := make(map[string]interface{})
		for _, sel := range gp.val {
			as, data := sel.getData()
			header[as] = data
		}

		for _, g := range gp.groupBy {
			header[g.key] = g.val
		}

		e := d.eventPool.Get()
		e.Fill(e.Meta(), header, []byte{})
		productFunc(e)
	}

	// clear group map
	if len(d.groupValue) > 0 {
		d.groupValue = make(map[string]Selected)
	}
}
