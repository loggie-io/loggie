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
	"github.com/loggie-io/loggie/pkg/util/eventops"
	"github.com/pkg/errors"
)

const (
	OneOfName     = "oneOf"
	OneOfUsageMsg = "usage: oneOf(key, value1, value2...)"
)

// OneOf check if the value of fields is one of these target values
type OneOf struct {
	field string
	value []string
}

func init() {
	RegisterCondition(OneOfName, func(args []string) (Condition, error) {
		return NewOneOf(args)
	})
}

func NewOneOf(args []string) (*OneOf, error) {
	if len(args) <= 1 {
		return nil, errors.Errorf("invalid args, %s", OneOfUsageMsg)
	}

	return &OneOf{
		field: args[0],
		value: args[1:],
	}, nil
}

func (one *OneOf) Check(e api.Event) bool {
	fieldVal := eventops.GetString(e, one.field)
	for _, v := range one.value {
		if v == fieldVal {
			return true
		}
	}

	return false
}
