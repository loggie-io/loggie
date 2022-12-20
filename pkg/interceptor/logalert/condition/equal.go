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
	"github.com/loggie-io/loggie/pkg/util/eventops"
)

const EQ = "eq"

func init() {
	RegisterCondition(EQ, Equal)
}

func Equal(input, target interface{}) (flag bool, err error) {
	v1, ok1 := eventops.NewNumber(input)
	v2, ok2 := eventops.NewNumber(target)
	if ok1 == nil && ok2 == nil {
		return v1.Equal(v2), nil
	}

	return input == target, nil
}
