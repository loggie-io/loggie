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
	LessName     = "less"
	LessUsageMsg = "usage: less(key, value)"
)

// Less check if the value of fields is Less to target value
type Less struct {
	field string
	value *eventops.Number
}

func init() {
	RegisterCondition(LessName, func(args []string) (Condition, error) {
		return NewLess(args)
	})
}

func NewLess(args []string) (*Less, error) {
	if len(args) != 2 {
		return nil, errors.Errorf("invalid args, %s", LessUsageMsg)
	}

	number, err := eventops.NewNumber(args[1])
	if err != nil {
		return nil, err
	}

	return &Less{
		field: args[0],
		value: number,
	}, nil
}

func (ls *Less) Check(e api.Event) bool {
	fieldVal, err := eventops.GetNumber(e, ls.field)
	if err != nil {
		log.Warn("parse field %s value %v to number error: %v", ls.field, fieldVal, err)
		return false
	}

	return fieldVal.Less(ls.value)
}
