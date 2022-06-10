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

func GetQueryPaths(query string) []string {
	paths := strings.Split(query, sep)
	return paths
}

func GetQueryUpperPaths(query string) ([]string, string) {
	paths := strings.Split(query, sep)
	if len(paths) < 2 {
		return []string{}, query
	}
	upper := paths[:len(paths)-1]
	last := paths[len(paths)-1:]
	lastQuery := last[0]

	return upper, lastQuery
}
