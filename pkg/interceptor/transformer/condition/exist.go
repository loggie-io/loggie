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
	ExistName     = "exist"
	ExistUsageMsg = "usage: exist(key)"
)

// Exist check if the fields value is null or not exist in the event
type Exist struct {
	field string
}

func init() {
	RegisterCondition(ExistName, func(args []string) (Condition, error) {
		return NewExist(args)
	})
}

func NewExist(args []string) (*Exist, error) {
	if len(args) != 1 {
		return nil, errors.Errorf("invalid args, %s", ExistUsageMsg)
	}

	return &Exist{
		field: args[0],
	}, nil
}

func (et *Exist) Check(e api.Event) bool {
	return eventops.Get(e, et.field) != nil
}
