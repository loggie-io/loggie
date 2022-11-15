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

package subcmd

import (
	"flag"
	"github.com/loggie-io/loggie/pkg/ops/dashboard"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/gui"
	"github.com/pkg/errors"
	"os"
)

const SubCommandInspect = "inspect"

var (
	inspectCmd *flag.FlagSet

	LoggiePort int
)

func init() {
	inspectCmd = flag.NewFlagSet(SubCommandInspect, flag.ExitOnError)

	inspectCmd.IntVar(&LoggiePort, "loggiePort", 9196, "Loggie http port")
}

func SwitchSubCommand() error {
	if len(os.Args) == 1 {
		return nil
	}
	switch os.Args[1] {
	case SubCommandInspect:
		if err := runInspect(); err != nil {
			return err
		}
		return errors.New("exit")
	}
	return nil
}

func runInspect() error {
	if len(os.Args) > 2 {
		if err := inspectCmd.Parse(os.Args[2:]); err != nil {
			return err
		}
	}

	d := dashboard.New(&gui.Config{
		LoggiePort: LoggiePort,
	})
	if err := d.Start(); err != nil {
		d.Stop()
		return err
	}

	return nil
}
