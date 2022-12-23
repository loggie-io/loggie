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

package journalctl

import (
	"bytes"
	"os/exec"
	"strings"

	"github.com/loggie-io/loggie/pkg/core/log"
)

type Command struct {
	Cmd []string
}

func NewJournalCtlCmd() *Command {
	command := &Command{}
	return command
}

func (c *Command) GetCmd() string {
	return strings.Join(c.Cmd, " ")
}

func (c *Command) Clear() {
	c.Cmd = []string{}
}

func Check() error {
	cmd := exec.Command("journalctl", "--version")
	_, err := cmd.CombinedOutput()
	if err != nil {
		log.Warn("journalctl not exist!")
		return err
	}
	return nil
}

func (c *Command) RunCmd(buffer *bytes.Buffer) error {
	cmd := exec.Command("journalctl", c.Cmd...)
	log.Debug("runing cmd %s", cmd.String())
	cmd.Stdout = buffer
	err := cmd.Run()
	if err != nil {
		log.Warn("fail to run cmd due to %s", err.Error())
		return err
	}
	return nil
}

func (c *Command) writeCmd(args ...string) *Command {
	for _, arg := range args {
		c.Cmd = append(c.Cmd, arg)
	}
	return c
}

func (c *Command) WithDir(dir string) *Command {
	return c.writeCmd("-D", dir)
}

func (c *Command) WithSince(since string) *Command {
	return c.writeCmd("--since", since)
}

func (c *Command) WithUntil(until string) *Command {
	return c.writeCmd("--until", until)
}

func (c *Command) WithUnit(unit string) *Command {
	return c.writeCmd("-u", unit)
}

func (c *Command) WithIdentifier(id string) *Command {
	return c.writeCmd("-t", id)
}

func (c *Command) WithOutputFormat(format string) *Command {
	return c.writeCmd("-o", format)
}

func (c *Command) WithNoPager() *Command {
	return c.writeCmd("--no-pager")
}
