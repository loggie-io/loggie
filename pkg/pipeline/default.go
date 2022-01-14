/*
Copyright 2021 Loggie Authors

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

package pipeline

import (
	"github.com/pkg/errors"
	"github.com/loggie-io/loggie/pkg/util"
)

var defaultConfigRaw ConfigRaw

func SetDefaultConfigRaw(defaults ConfigRaw) {
	defaultConfigRaw = defaults
}

func GetDefaultConfigRaw() (*ConfigRaw, error) {
	rawCopy := &ConfigRaw{}
	err := util.Clone(defaultConfigRaw, rawCopy)
	if err != nil {
		return nil, errors.WithMessage(err, "get default config failed")
	}
	return rawCopy, nil
}
