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

package transformer

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/interceptor/transformer/action"
	"github.com/loggie-io/loggie/pkg/interceptor/transformer/condition"
	"github.com/pkg/errors"
)

type Actions struct {
	steps []*ActionStep
}

func NewActions(config []StepConfig) (*Actions, error) {
	if len(config) == 0 {
		return &Actions{}, nil
	}

	// initial action steps
	var steps []*ActionStep
	for _, c := range config {
		step, err := NewActionStep(c)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}

	return &Actions{
		steps: steps,
	}, nil
}

func (a *Actions) Exec(e api.Event) error {
	if len(a.steps) == 0 {
		return nil
	}

	for _, step := range a.steps {
		if err := step.exec(e); err != nil {
			if errors.Is(err, action.ErrorReturn) {
				return nil
			}

			return err
		}
	}
	return nil
}

// ActionStep abstract all the action including conditionAction and simpleAction
type ActionStep struct {
	ConditionStep *ConditionActionStep // type conditionAction
	SimpleAction  *action.Instance     // type simpleAction
}

func NewActionStep(config StepConfig) (*ActionStep, error) {
	as := &ActionStep{}

	// new condition action
	if config.withCondition() {
		condStep, err := NewConditionActionStep(config.ConditionConfig)
		if err != nil {
			return as, err
		}

		as.ConditionStep = condStep
		return as, nil
	}

	// new simple action
	act, err := action.GetAction(config.ActionConfig)
	if err != nil {
		return nil, err
	}

	as.SimpleAction = act
	return as, nil
}

func (as *ActionStep) exec(e api.Event) error {
	// take action with condition
	if as.withCondition() {
		if err := as.ConditionStep.exec(e); err != nil {
			return err
		}
		return nil
	}

	// or we take simple action
	if err := as.SimpleAction.Exec(e); err != nil {
		return err
	}

	return nil
}

// withCondition means this action would check condition before run
func (as *ActionStep) withCondition() bool {
	if as.ConditionStep == nil {
		return false
	}

	return true
}

//----------------- condition action --------------//

type ConditionActionStep struct {
	ConditionExpression string
	Conditions          []*condition.Instance
	Connector           string // AND / OR

	Then []*action.Instance
	Else []*action.Instance
}

func NewConditionActionStep(config condition.Config) (*ConditionActionStep, error) {

	step := &ConditionActionStep{}

	// init `if` conditions
	conditions, connector, err := condition.GetConditions(config.If)
	if err != nil {
		return step, err
	}
	step.ConditionExpression = config.If
	step.Conditions = conditions
	step.Connector = connector

	// make `then` actions
	thenActions, err := makeActionInstances(config.Then)
	if err != nil {
		return nil, err
	}
	step.Then = thenActions

	// make `else` actions
	if len(config.Else) > 0 {
		elseActions, err := makeActionInstances(config.Else)
		if err != nil {
			return nil, err
		}
		step.Else = elseActions
	}

	return step, nil
}

func makeActionInstances(config []action.Config) ([]*action.Instance, error) {
	var ins []*action.Instance
	for _, then := range config {
		ac, err := action.GetAction(then)
		if err != nil {
			return nil, err
		}

		ins = append(ins, ac)
	}

	return ins, nil
}

func (ca *ConditionActionStep) satisfied(e api.Event) bool {
	// AND all the conditions must return true
	if ca.Connector == condition.AND {
		for _, cond := range ca.Conditions {
			if !checkWithNegative(e, cond.Condition, cond.Negative) {
				return false
			}
		}

		return true
	}

	// OR need one of the conditions return true
	for _, cond := range ca.Conditions {
		if checkWithNegative(e, cond.Condition, cond.Negative) {
			return true
		}
	}

	return false
}

func checkWithNegative(event api.Event, cond condition.Condition, negative bool) bool {
	if negative {
		return !cond.Check(event)
	}
	return cond.Check(event)
}

func (ca *ConditionActionStep) exec(e api.Event) error {
	// meet the condition, take `then` actions
	satisfy := ca.satisfied(e)
	log.Debug("if condition: %s, return %t", ca.ConditionExpression, satisfy)

	if satisfy {
		return multipleActionExec(ca.Then, e)
	}

	// or take `else` actions
	return multipleActionExec(ca.Else, e)
}

func multipleActionExec(acts []*action.Instance, e api.Event) error {
	if len(acts) == 0 {
		return nil
	}

	for _, a := range acts {
		err := a.Exec(e)
		if err != nil {
			return err
		}
	}
	return nil
}
