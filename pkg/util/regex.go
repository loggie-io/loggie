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

package util

import (
	"regexp"
	"strings"
)

func InitMatcher(pattern string) [][]string {
	// TODO regexp optimize
	indexReg := regexp.MustCompile(`\${(.+?)}`)
	return indexReg.FindAllStringSubmatch(pattern, -1)
}


func CompilePatternWithJavaStyle(pattern string) *regexp.Regexp {
	// compile java、c# named capturing groups style
	if strings.Contains(pattern, "?<") {
		pattern = strings.ReplaceAll(pattern, "?<", "?P<")
	}
	return regexp.MustCompile(pattern)
}

func MatchGroup(pattern string, context string) (paramsMap map[string]string) {
	compRegEx := CompilePatternWithJavaStyle(pattern)
	return MatchGroupWithRegex(compRegEx, context)
}

func MatchGroupWithRegex(compRegEx *regexp.Regexp, context string) (paramsMap map[string]string) {
	match := compRegEx.FindStringSubmatch(context)
	l := len(match)
	if l == 0 {
		return
	}
	paramsMap = make(map[string]string, l)
	for i, name := range compRegEx.SubexpNames() {
		if i > 0 && i <= l {
			paramsMap[name] = match[i]
		}
	}
	return
}
