package journalctl

import (
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

func (c *Command) RunCmd() ([]byte, error) {
	cmd := exec.Command("journalctl", c.Cmd...)
	log.Debug("runing cmd %s", cmd.String())
	bytes, err := cmd.CombinedOutput()
	if err != nil {
		log.Warn("fail to run cmd due to %s", err.Error())
		return nil, err
	}
	return bytes, nil
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
