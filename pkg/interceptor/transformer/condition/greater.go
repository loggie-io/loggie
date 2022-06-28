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

package condition

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/eventops"
	"github.com/pkg/errors"
)

const (
	GreaterName     = "greater"
	GreaterUsageMsg = "usage: greater(key, value)"
)

// Greater check if the value of fields is Greater to target value
type Greater struct {
	field string
	value *eventops.Number
}

func init() {
	RegisterCondition(GreaterName, func(args []string) (Condition, error) {
		return NewGreater(args)
	})
}

func NewGreater(args []string) (*Greater, error) {
	if len(args) != 2 {
		return nil, errors.Errorf("invalid args, %s", GreaterUsageMsg)
	}

	number, err := eventops.NewNumber(args[1])
	if err != nil {
		return nil, err
	}

	return &Greater{
		field: args[0],
		value: number,
	}, nil
}

func (gt *Greater) Check(e api.Event) bool {
	fieldVal, err := eventops.GetNumber(e, gt.field)
	if err != nil {
		log.Warn("parse field %s value %v to number error: %v", gt.field, fieldVal, err)
		return false
	}

	return fieldVal.Greater(gt.value)
}
