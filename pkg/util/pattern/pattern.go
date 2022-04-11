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

package pattern

import (
	"regexp"
	"strings"
)

const (
	Indicator       = '$'
	SeparatorPrefix = '{'
	SeparatorSuffix = '}'
)

// GetSplits
// eg: target="/var/log/${pod.uid}/${pod.name}/"
//     returns ["var/log/", "/", "/"] and ["pod.uid", "pod.name"]
func GetSplits(target string) (splitStr []string, matchers []string) {

	var splitStrList []string
	var matcherList []string

	inMatcher := false
	var splitStrBuilder strings.Builder
	var matcherBuilder strings.Builder

	for i, s := range target {
		if !inMatcher {
			if s == Indicator && i < len(target)-1 && target[i+1] == SeparatorPrefix { // not the last rune, and the next is '{'
				if splitStrBuilder.Len() > 0 {
					splitStrList = append(splitStrList, splitStrBuilder.String())
					splitStrBuilder.Reset()
				}
				inMatcher = true
				continue
			}

			splitStrBuilder.WriteRune(s)
			continue
		}

		// in match record
		if s == Indicator || s == SeparatorPrefix { // ignore
			continue
		}

		if s == SeparatorSuffix { // end the matcher
			if matcherBuilder.Len() > 0 {
				matcherList = append(matcherList, matcherBuilder.String())
				matcherBuilder.Reset()
			}
			inMatcher = false
			continue
		}

		matcherBuilder.WriteRune(s)
	}

	if splitStrBuilder.Len() > 0 {
		splitStrList = append(splitStrList, splitStrBuilder.String())
	}
	return splitStrList, matcherList
}

// Extract
// eg: input="/var/log/76fb94cbb5/tomcat/", splitsStr=["/var/log/", "/", "/"]
//     return ["76fb94cbb5", "tomcat"]
func Extract(input string, splitsStr []string) []string {
	var ret []string
	segment := input
	for _, split := range splitsStr {
		lastIndex := strings.Index(segment, split) + len(split)
		sub := segment[:lastIndex]
		segment = segment[lastIndex:]
		val := strings.TrimSuffix(sub, split)
		if val != "" {
			ret = append(ret, val)
		}
	}

	return ret
}

const matchExpr = `\${(.+?)}`

func InitMatcher(pattern string) ([][]string, error) {
	reg, err := regexp.Compile(matchExpr)
	if err != nil {
		return nil, err
	}
	return reg.FindAllStringSubmatch(pattern, -1), nil
}

func MustInitMatcher(pattern string) [][]string {
	// TODO regexp optimize
	reg := regexp.MustCompile(matchExpr)
	return reg.FindAllStringSubmatch(pattern, -1)
}
