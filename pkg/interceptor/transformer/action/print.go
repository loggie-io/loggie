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
	eventer "github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/json"
	"github.com/pkg/errors"
)

const (
	PrintName     = "print"
	PrintUsageMsg = "usage: print()"
)

func init() {
	RegisterAction(PrintName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewPrint(args)
	})
}

type Print struct {
}

func NewPrint(args []string) (*Print, error) {
	aCount := len(args)
	if aCount != 0 {
		return nil, errors.Errorf("invalid args, %s", PrintUsageMsg)
	}

	return &Print{}, nil
}

func (f *Print) act(e api.Event) error {
	// TODO Encapsulated this into a function together with sink json codec
	header := make(map[string]interface{})
	for k, v := range e.Header() {
		header[k] = v
	}

	if len(e.Body()) != 0 {
		// put body in header
		header[eventer.Body] = string(e.Body())
	}

	out, err := json.Marshal(header)
	if err != nil {
		return err
	}

	log.Info("event: %s", string(out))
	return nil
}
