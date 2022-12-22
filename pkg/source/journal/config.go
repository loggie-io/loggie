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
	AddAllMeta           bool                 `yaml:"addAllMeta,omitempty"`
	DbConfig             persistence.DbConfig `yaml:"db,omitempty"`
}

func (c *Config) Validate() error {
	if len(c.Dir) == 0 {
		return errors.New("dir empty")
	}

	if len(c.StartTime) > 0 {
		_, err := time.ParseInLocation(TimeFmt, c.StartTime, time.Local)
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
