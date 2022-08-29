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

package runtime

import (
	"strings"
)

const (
	sep          = '.'
	connectStart = '['
	connectEnd   = ']'
)

func GetQueryPaths(query string) []string {
	var result []string
	var sb strings.Builder
	connectorMode := false
	for _, s := range query {
		if s == sep && !connectorMode {
			result = append(result, sb.String())
			sb.Reset()
			continue

		} else if s == connectStart {
			connectorMode = true
			continue

		} else if s == connectEnd {
			connectorMode = false
			continue
		}

		// add sub string
		sb.WriteRune(s)
	}

	result = append(result, sb.String())
	return result
}

func GetQueryUpperPaths(query string) ([]string, string) {
	paths := GetQueryPaths(query)
	if len(paths) < 2 {
		return []string{}, query
	}
	upper := paths[:len(paths)-1]
	last := paths[len(paths)-1:]
	lastQuery := last[0]

	return upper, lastQuery
}
