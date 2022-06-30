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
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/pkg/errors"
)

const (
	FmtName     = "fmt"
	FmtUsageMsg = "usage: fmt(key)"
)

func init() {
	RegisterAction(FmtName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewFmt(args, extra)
	})
}

type Fmt struct {
	key   string
	p     *pattern.Pattern
	extra *fmtExtra
}

type fmtExtra struct {
	Pattern string `yaml:"pattern,omitempty"`
}

func (f *fmtExtra) compile() (*pattern.Pattern, error) {
	if f.Pattern == "" {
		return nil, errors.New("fmt pattern is required")
	}

	p, err := pattern.Init(f.Pattern)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func NewFmt(args []string, extra cfg.CommonCfg) (*Fmt, error) {
	aCount := len(args)
	if aCount != 1 {
		return nil, errors.Errorf("invalid args, %s", FmtUsageMsg)
	}

	extraCfg := &fmtExtra{}
	if err := cfg.Unpack(extra, extraCfg); err != nil {
		return nil, err
	}

	p, err := extraCfg.compile()
	if err != nil {
		return nil, err
	}

	return &Fmt{
		key:   args[0],
		p:     p,
		extra: extraCfg,
	}, nil
}

// TODO event body should also support to be reformat
func (f *Fmt) act(e api.Event) error {
	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	obj := runtime.NewObject(header)
	result, err := f.p.WithObject(obj).Render()
	if err != nil {
		return err
	}

	obj.SetPath(f.key, result)
	return nil
}
