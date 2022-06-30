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
	EqualName     = "equal"
	EqualUsageMsg = "usage: equal(key, target)"
)

// Equal check if the value of fields is equal to target value
type Equal struct {
	field string
	value string
}

func init() {
	RegisterCondition(EqualName, func(args []string) (Condition, error) {
		return NewEqual(args)
	})
}

func NewEqual(args []string) (*Equal, error) {
	if len(args) != 2 {
		return nil, errors.Errorf("invalid args, %s", EqualUsageMsg)
	}

	return &Equal{
		field: args[0],
		value: args[1],
	}, nil
}

func (eq *Equal) Check(e api.Event) bool {
	return eq.value == eventops.Get(e, eq.field)
}
