package journal

import (
	"errors"
	"time"

	journalctl "github.com/loggie-io/loggie/pkg/source/journal/ctl"
	"github.com/loggie-io/loggie/pkg/util/persistence"
)

type Config struct {
	Dir                  string               `yaml:"dir"`
	Unit                 string               `yaml:"unit,omitempty"`
	Identifier           string               `yaml:"identifier,omitempty"`
	StartTime            string               `yaml:"startTime,omitempty"`
	HistorySplitDuration time.Duration        `yaml:"historySplitDuration,omitempty" default:"1h"`
	CollectInterval      int                  `yaml:"collectInterval,omitempty" default:"10"`
	AddMeta              map[string]string    `yaml:"addMeta,omitempty"`
	DbConfig             persistence.DbConfig `yaml:"db,omitempty"`
}

func (c *Config) Validate() error {
	if len(c.Dir) == 0 {
		return errors.New("dir empty")
	}

	if len(c.StartTime) > 0 {
		_, err := time.ParseInLocation(timeFmt, c.StartTime, time.Local)
		if err != nil {
			return err
		}
	}

	err := journalctl.Check()
	if err != nil {
		return err
	}

	return nil
}
