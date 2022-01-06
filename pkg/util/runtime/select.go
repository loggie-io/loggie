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
	"github.com/pkg/errors"
	"loggie.io/loggie/pkg/util"
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

// PatternSelect
// eg: pattern: aa-${field.bb}-${+YYYY.MM.DD}
// field.bb in event is xx
// would be format to: aa-xx-2021.07.04
func PatternSelect(obj *Object, pattern string, matcher [][]string) (string, error) {
	if len(matcher) == 0 {
		return pattern, nil
	}
	var oldNew []string

	for _, m := range matcher {
		keyWrap := m[0] // ${fields.xx}
		key := m[1]     // fields.xx

		alt, err := getNew(obj, key)
		if err != nil {
			return "", errors.WithMessage(err, "replace pattern error")
		}
		// add old
		oldNew = append(oldNew, keyWrap)
		// add new
		oldNew = append(oldNew, alt)
	}

	replacer := strings.NewReplacer(oldNew...)
	res := replacer.Replace(pattern)

	return res, nil
}

const timeToken = "+"

func getNew(obj *Object, key string) (string, error) {
	if strings.HasPrefix(key, timeToken) { // timeFormat
		return util.TimeFormatNow(strings.TrimLeft(key, timeToken)), nil
	}

	val, err := obj.GetPath(key).String()
	if err != nil {
		return "", err
	}
	return val, nil
}
