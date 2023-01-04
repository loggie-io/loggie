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
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/eventops"
)

const (
	OperatorCount = "count"
	OperatorSum = "sum"
	OperatorAvg = "avg"
	OperatorMax = "max"
	OperatorMin = "min"
)


type Operator interface {
	getData() (as string, data interface{})

	// SetData 根据不同的operator，进行相应的聚合计算
	compute(event api.Event, key string) error
}

func initOperatorValue(selected []AggregateSelectConfig, e api.Event) map[string]Operator {
	val := make(map[string]Operator)
	for _, s := range selected {
		var op Operator
		switch s.Operator {
		case OperatorCount:
			op = NewCount(s.As)

		case OperatorSum:
			op = NewSum(s.As)

		}

		if op == nil {
			continue
		}
		if err := op.compute(e, s.Key); err != nil {
			log.Error("%+v", err)
			continue
		}

		val[s.Key] = op
	}

	return val
}

type Count struct {
	data int64
	as string
}

func NewCount(as string) *Count {
	return &Count{
		data: 0,
		as: as,
	}
}

func (c *Count) getData() (string, interface{}) {
	return c.as, c.data
}

func (c *Count) compute(event api.Event, key string) error {
	// 如果不统计某个key，则计算整个event
	if key == "" {
		c.data = c.data + 1
		return nil
	}

	// 只有key有值才会增加count值
	ret := eventops.Get(event, key)
	if ret != "" {
		c.data = c.data + 1
	}
	return nil
}

type Sum struct {
	data *eventops.Number
	as string
}

func NewSum(as string) *Sum {
	n, err := eventops.NewNumber(0)
	if err != nil {
		log.Error("new number failed： %+v", err)
		return nil
	}
	return &Sum{
		data: n,
		as: as,
	}
}

func (s *Sum) getData() (string, interface{}) {
	return s.as, s.data.Float64()
}

func (s *Sum) compute(event api.Event, key string) error {
	num, err := eventops.GetNumber(event, key)
	if err != nil {
		return err
	}
	s.data.Add(num)
	return nil
}
