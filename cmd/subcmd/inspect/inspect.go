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

package inspect

import (
	"flag"
	"github.com/loggie-io/loggie/pkg/ops/dashboard"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/gui"
	"os"
)

const SubCommandInspect = "inspect"

var (
	inspectCmd *flag.FlagSet
	loggiePort int
)

func init() {
	inspectCmd = flag.NewFlagSet(SubCommandInspect, flag.ExitOnError)
	inspectCmd.IntVar(&loggiePort, "loggiePort", 9196, "Loggie http port")
}

func RunInspect() error {
	if len(os.Args) > 2 {
		if err := inspectCmd.Parse(os.Args[2:]); err != nil {
			return err
		}
	}

	d := dashboard.New(&gui.Config{
		LoggiePort: loggiePort,
	})
	if err := d.Start(); err != nil {
		d.Stop()
		return err
	}

	return nil
}
