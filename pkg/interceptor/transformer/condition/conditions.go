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
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/interceptor/transformer/action"
	expr "github.com/loggie-io/loggie/pkg/interceptor/transformer/expression"
	"github.com/pkg/errors"
	"strings"
)

const (
	AND      = "AND"
	OR       = "OR"
	NEGATIVE = "NOT"
)

type Config struct {
	If   string          `yaml:"if,omitempty"`
	Then []action.Config `yaml:"then,omitempty"`
	Else []action.Config `yaml:"else,omitempty"`
}

func (c *Config) Validate() error {
	if c.If == "" {
		return errors.New("if condition is required")
	}
	if len(c.Then) == 0 {
		return errors.New("then action is required")
	}

	for _, t := range c.Then {
		if err := t.Validate(); err != nil {
			return err
		}
	}

	if len(c.Else) > 0 {
		for _, t := range c.Else {
			if err := t.Validate(); err != nil {
				return err
			}
		}
	}
	return nil
}

type Condition interface {
	Check(e api.Event) bool
}

//---------------- register condition -------------//

type Factory func(args []string) (Condition, error)

var conditionRegister = make(map[string]Factory)

func RegisterCondition(name string, f Factory) {
	_, ok := conditionRegister[name]
	if ok {
		log.Panic("condition %s is duplicated", name)
	}
	conditionRegister[name] = f
}

//---------------- condition expression -----------//

type Instance struct {
	Condition
	Negative bool
	Express  *expr.Expression
}

// GetConditions ..
func GetConditions(expression string) ([]*Instance, string, error) {
	instances, connector, err := getSubExpressions(expression)
	if err != nil {
		return nil, "", err
	}

	for _, ins := range instances {
		factory, ok := conditionRegister[ins.Express.Name]
		if !ok {
			return nil, connector, errors.Errorf("condition %s is not exist", ins.Express.Name)
		}

		c, err := factory(ins.Express.Args)
		if err != nil {
			return nil, connector, err
		}

		ins.Condition = c
	}

	return instances, connector, nil
}

func getSubExpressions(expression string) ([]*Instance, string, error) {
	var expressions []*Instance
	var connector string

	var ex []string
	if strings.Contains(expression, AND) {
		sub := strings.Split(expression, AND)
		connector = AND
		ex = append(ex, sub...)
	} else if strings.Contains(expression, OR) {
		sub := strings.Split(expression, OR)
		connector = OR
		ex = append(ex, sub...)
	} else {
		ex = append(ex, expression)
	}

	for _, e := range ex {
		var negative bool
		e = strings.TrimSpace(e)
		if strings.HasPrefix(e, NEGATIVE) {
			e = strings.TrimPrefix(e, NEGATIVE)
			negative = true
		}

		express, err := expr.ParseExpression(e)
		if err != nil {
			return nil, connector, err
		}

		conditionExpr := &Instance{
			Express:  express,
			Negative: negative,
		}

		expressions = append(expressions, conditionExpr)
	}

	return expressions, connector, nil
}
