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

package concurrency

import (
	"strconv"
	"strings"

	"github.com/loggie-io/loggie/pkg/core/log"
)

type Config struct {
	Enable    bool       `yaml:"enable,omitempty"`
	Goroutine *Goroutine `yaml:"goroutine,omitempty"`
	Rtt       *Rtt       `yaml:"rtt,omitempty"`
	Ratio     *Ratio     `yaml:"ratio,omitempty"`
	Duration  *Duration  `yaml:"duration,omitempty"`
}

func (c *Config) Validate() error {
	if !c.Enable {
		return nil
	}

	if c.Rtt != nil {
		log.Debug("check blockJudgeThreshold")
		blockJudgeThreshold := c.Rtt.BlockJudgeThreshold
		if len(blockJudgeThreshold) > 0 {
			if strings.Contains(blockJudgeThreshold, "%") {
				_, err := strconv.ParseFloat(blockJudgeThreshold[:len(blockJudgeThreshold)-1], 64)
				if err != nil {
					return err
				}
			} else {
				_, err := strconv.ParseFloat(blockJudgeThreshold, 64)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (c *Config) SetDefaults() {
	if c.Goroutine == nil {
		c.Goroutine = &Goroutine{
			InitThreshold:    16,
			MaxGoroutine:     30,
			UnstableTolerate: 3,
			ChannelLenOfCap:  0.4,
		}
	} else {
		if c.Goroutine.InitThreshold == 0 {
			c.Goroutine.InitThreshold = 16
		}
		if c.Goroutine.MaxGoroutine == 0 {
			c.Goroutine.MaxGoroutine = 30
		}
		if c.Goroutine.UnstableTolerate == 0 {
			c.Goroutine.UnstableTolerate = 3
		}
		if c.Goroutine.ChannelLenOfCap == 0 {
			c.Goroutine.ChannelLenOfCap = 0.4
		}
	}

	if c.Rtt == nil {
		c.Rtt = &Rtt{
			BlockJudgeThreshold: "120%",
			NewRttWeigh:         0.5,
		}
	} else {
		if len(c.Rtt.BlockJudgeThreshold) == 0 {
			c.Rtt.BlockJudgeThreshold = "120%"
		}
		if c.Rtt.NewRttWeigh == 0 {
			c.Rtt.NewRttWeigh = 0.5
		}
	}

	if c.Ratio == nil {
		c.Ratio = &Ratio{
			Multi:             2,
			Linear:            2,
			LinearWhenBlocked: 4,
		}
	} else {
		if c.Ratio.Multi == 0 {
			c.Ratio.Multi = 2
		}
		if c.Ratio.Linear == 0 {
			c.Ratio.Linear = 2
		}
		if c.Ratio.LinearWhenBlocked == 0 {
			c.Ratio.LinearWhenBlocked = 4
		}
	}

	if c.Duration == nil {
		c.Duration = &Duration{
			Unstable: 15,
			Stable:   30,
		}
	} else {
		if c.Duration.Unstable == 0 {
			c.Duration.Unstable = 15
		}
		if c.Duration.Stable == 0 {
			c.Duration.Stable = 30
		}
	}
}

type Goroutine struct {
	InitThreshold    int     `yaml:"initThreshold,omitempty" default:"16" validate:"gte=1"`
	MaxGoroutine     int     `yaml:"maxGoroutine,omitempty" default:"30" validate:"gte=1"`
	UnstableTolerate int     `yaml:"unstableTolerate,omitempty" default:"3" validate:"gte=1"`
	ChannelLenOfCap  float64 `yaml:"channelLenOfCap,omitempty" default:"0.4" validate:"gt=0"`
}

type Rtt struct {
	BlockJudgeThreshold string  `yaml:"blockJudgeThreshold,omitempty" default:"120%"`
	NewRttWeigh         float64 `yaml:"newRttWeigh,omitempty" default:"0.5" validate:"gte=0,lte=1"`
}

type Ratio struct {
	Multi             int `yaml:"multi,omitempty" default:"2" validate:"gt=1"`
	Linear            int `yaml:"linear,omitempty" default:"2" validate:"gt=1"`
	LinearWhenBlocked int `yaml:"linearWhenBlocked,omitempty" default:"4" validate:"gt=1"`
}

type Duration struct {
	Unstable int `yaml:"unstable,omitempty" default:"15" validate:"gte=1"`
	Stable   int `yaml:"stable,omitempty" default:"30" validate:"gte=1"`
}
