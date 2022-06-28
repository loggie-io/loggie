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

package action

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/log"
	expr "github.com/loggie-io/loggie/pkg/interceptor/transformer/expression"
	"github.com/pkg/errors"
)

const (
	HeaderRoot = "_root"
)

type Config struct {
	Expression  string        `yaml:"action,omitempty"`
	IgnoreError bool          `yaml:"ignoreError,omitempty"`
	Extra       cfg.CommonCfg `yaml:",inline,omitempty"`
}

func (c *Config) Validate() error {
	if c.Expression == "" {
		return errors.New("action expression is required")
	}
	return nil
}

type Instance struct {
	Name string
	Action
	Config Config
}

type Action interface {
	act(e api.Event) error
}

type Factory func(args []string, extra cfg.CommonCfg) (Action, error)

var actionRegister = make(map[string]Factory)

func RegisterAction(name string, f Factory) {
	_, ok := actionRegister[name]
	if ok {
		log.Panic("action %s is duplicated", name)
	}
	actionRegister[name] = f
}

// GetAction ..
func GetAction(config Config) (*Instance, error) {
	e, err := expr.ParseExpression(config.Expression)
	if err != nil {
		return nil, err
	}

	factory, ok := actionRegister[e.Name]
	if !ok {
		return nil, errors.Errorf("condition %s is not exist", e.Name)
	}

	action, err := factory(e.Args, config.Extra)
	if err != nil {
		return nil, err
	}

	return &Instance{
		Name:   e.Name,
		Config: config,
		Action: action,
	}, nil
}

func (a *Instance) Exec(e api.Event) error {
	err := a.Action.act(e)
	if err == nil {
		log.Debug("action: %s execute success", a.Name)
		return nil
	}

	if a.Config.IgnoreError {
		log.Debug("action: %s execute failed, but ignored error", a.Name)
		return nil
	}

	return errors.WithMessagef(err, "failed to execute action %s", a.Name)
}
