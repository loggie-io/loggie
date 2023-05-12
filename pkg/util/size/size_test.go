/*
Copyright 2023 Loggie Authors

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

package size

import (
	"github.com/loggie-io/loggie/pkg/util/yaml"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

type ConfigTest struct {
	Size Size `yaml:"size"`
}

func TestSizeYAML(t *testing.T) {
	tests := []struct {
		name        string
		yaml        string
		expected    Size
		expectedErr bool
	}{
		{
			name:     "Bytes",
			yaml:     "size: 123B",
			expected: Size{Bytes: 123},
		},
		{
			name:     "KB",
			yaml:     "size: 1.23KB",
			expected: Size{Bytes: 1259},
		},
		{
			name:     "MB",
			yaml:     "size: 1.23MB",
			expected: Size{Bytes: 1289748},
		},
		{
			name:     "GB",
			yaml:     "size: 1.23GB",
			expected: Size{Bytes: 1320702443},
		},
		{
			name:        "InvalidFormat",
			yaml:        "size: 1.23TB",
			expected:    Size{},
			expectedErr: true,
		},
		{
			name:        "Negative",
			yaml:        "size: -1",
			expected:    Size{},
			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var config ConfigTest
			err := yaml.Unmarshal([]byte(test.yaml), &config)
			if test.expected.Bytes > 0 {
				assert.NoError(t, err)
				assert.Equal(t, test.expected, config.Size)
			} else {
				assert.Error(t, err)
				assert.Zero(t, config.Size.Bytes)
			}

			yamlBytes, err := yaml.Marshal(&config)
			if !test.expectedErr {
				assert.NoError(t, err)
				assert.Equal(t, test.yaml, strings.TrimSpace(string(yamlBytes)))
			}
		})
	}
}
