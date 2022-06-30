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
	HasPrefixName     = "hasPrefix"
	HasPrefixUsageMsg = "usage: hasPrefix(key, prefix)"
)

// HasPrefix check if the value of fields has target prefix
type HasPrefix struct {
	field  string
	prefix string
}

func init() {
	RegisterCondition(HasPrefixName, func(args []string) (Condition, error) {
		return NewHasPrefix(args)
	})
}

func NewHasPrefix(args []string) (*HasPrefix, error) {
	if len(args) != 2 {
		return nil, errors.Errorf("invalid args, %s", HasPrefixUsageMsg)
	}

	return &HasPrefix{
		field:  args[0],
		prefix: args[1],
	}, nil
}

func (p *HasPrefix) Check(e api.Event) bool {
	fieldVal := eventops.GetString(e, p.field)
	return strings.HasPrefix(fieldVal, p.prefix)
}
