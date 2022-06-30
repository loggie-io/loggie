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
	"strings"
)

const (
	ContainName     = "contain"
	ContainUsageMsg = "usage: contain(key, target)"
)

// Contain check if the value of fields is Contain to target value
type Contain struct {
	field string
	value string
}

func init() {
	RegisterCondition(ContainName, func(args []string) (Condition, error) {
		return NewContain(args)
	})
}

func NewContain(args []string) (*Contain, error) {
	if len(args) != 2 {
		return nil, errors.Errorf("invalid args, %s", ContainUsageMsg)
	}

	return &Contain{
		field: args[0],
		value: args[1],
	}, nil
}

func (c *Contain) Check(e api.Event) bool {
	fieldVal := eventops.GetString(e, c.field)
	return strings.Contains(fieldVal, c.value)
}
