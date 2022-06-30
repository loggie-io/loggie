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
	codecjson "github.com/loggie-io/loggie/pkg/sink/codec/json"
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
	codec := codecjson.NewJson()
	out, err := codec.Encode(e)
	if err != nil {
		return err
	}

	log.Info("event: %s", string(out))
	return nil
}
