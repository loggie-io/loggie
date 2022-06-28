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

package expression

import (
	"github.com/pkg/errors"
	"regexp"
	"strings"
)

var expressionRegex = regexp.MustCompile(`^(?P<action>\w+)\(.*\)$`)

type Expression struct {
	Name string
	Args []string
}

// ParseExpression ..
func ParseExpression(expression string) (*Expression, error) {
	exp := strings.TrimSpace(expression)

	name, err := parseName(exp)
	if err != nil {
		return nil, err
	}
	args := parseArgs(name, exp)

	return &Expression{
		Name: name,
		Args: args,
	}, nil
}

func parseName(expression string) (string, error) {
	match := expressionRegex.FindStringSubmatch(expression)
	if len(match) != 2 { // there should only one condition name
		return "", errors.Errorf("expression %s is invalid", expression)
	}

	return match[1], nil
}

func parseArgs(name string, expression string) []string {
	withoutPrefix := strings.TrimPrefix(expression, name+"(")
	prune := strings.TrimSuffix(withoutPrefix, ")")
	splitParams := strings.Split(prune, ",")

	var args []string
	for _, s := range splitParams {
		p := strings.TrimSpace(s)
		if p == "" {
			continue
		}
		args = append(args, p)
	}
	return args
}
