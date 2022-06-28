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
	"regexp"
)

const (
	MatchName     = "match"
	MatchUsageMsg = "usage: match(key, regex)"
)

// Match check whether the field value contains any match of the regular expression
type Match struct {
	field   string
	pattern *regexp.Regexp
}

func init() {
	RegisterCondition(MatchName, func(args []string) (Condition, error) {
		return NewMatch(args)
	})
}

func NewMatch(args []string) (*Match, error) {
	if len(args) != 2 {
		return nil, errors.Errorf("invalid args, %s", MatchUsageMsg)
	}

	regex, err := regexp.Compile(args[1])
	if err != nil {
		return nil, err
	}

	return &Match{
		field:   args[0],
		pattern: regex,
	}, nil
}

func (m *Match) Check(e api.Event) bool {
	fieldVal := eventops.GetString(e, m.field)
	return m.pattern.MatchString(fieldVal)
}
