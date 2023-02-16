/*
Copyright 2023 Loggie Authors

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

package subcmd

import (
	"github.com/loggie-io/loggie/cmd/subcmd/genfiles"
	"github.com/loggie-io/loggie/cmd/subcmd/inspect"
	"github.com/pkg/errors"
	"os"
)

func SwitchSubCommand() error {
	if len(os.Args) == 1 {
		return nil
	}
	switch os.Args[1] {
	case inspect.SubCommandInspect:
		if err := inspect.RunInspect(); err != nil {
			return err
		}

	case genfiles.SubCommandGenFiles:
		if err := genfiles.RunGenFiles(); err != nil {
			return err
		}
	}

	return errors.New("exit")
}
