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
	"github.com/loggie-io/loggie/pkg/util/eventops"
	"github.com/pkg/errors"
	"strconv"
	"time"
)

const (
	TimestampName     = "timestamp"
	TimestampUsageMsg = "usage: timestamp(key)"

	LayoutUnix   = "unix"
	LayoutUnixMs = "unix_ms"
)

func init() {
	RegisterAction(TimestampName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewTimestamp(args, extra)
	})
}

type Timestamp struct {
	key   string
	extra *timestampExtra
}

type timestampExtra struct {
	FromLayout   string `yaml:"fromLayout,omitempty" validate:"required"` // support LayoutUnix LayoutUnixMs
	FromLocation string `yaml:"fromLocation,omitempty"`                   // "" indicate UTC, also support `Local`
	ToLayout     string `yaml:"toLayout,omitempty" validate:"required"`
	ToLocation   string `yaml:"toLocation,omitempty"`
}

func NewTimestamp(args []string, extra cfg.CommonCfg) (*Timestamp, error) {
	aCount := len(args)
	if aCount != 1 {
		return nil, errors.Errorf("invalid args, %s", TimestampUsageMsg)
	}

	timeExtra := &timestampExtra{}
	if err := cfg.UnpackDefaultsAndValidate(extra, timeExtra); err != nil {
		return nil, err
	}

	return &Timestamp{
		key:   args[0],
		extra: timeExtra,
	}, nil
}

func (t *Timestamp) act(e api.Event) error {

	extra := t.extra
	fromVal := eventops.GetString(e, t.key)

	// parse time
	var timestamp = time.Time{}
	switch extra.FromLayout {
	case LayoutUnix:
		i, err := strconv.ParseInt(fromVal, 10, 64)
		if err != nil {
			return err
		}
		fromTime := time.Unix(i, 0)
		timestamp = fromTime

	case LayoutUnixMs:
		i, err := strconv.ParseInt(fromVal, 10, 64)
		if err != nil {
			return err
		}
		fromTime := time.UnixMilli(i)
		timestamp = fromTime

	default:
		fromLocation, err := time.LoadLocation(extra.FromLocation)
		if err != nil {
			return errors.WithMessagef(err, "load location %s from field %s error", extra.FromLocation, t.key)
		}
		fromTime, err := time.ParseInLocation(extra.FromLayout, fromVal, fromLocation)
		if err != nil {
			return errors.WithMessagef(err, "parse timestamp with layout %s from field %s error", extra.FromLayout, t.key)
		}
		timestamp = fromTime

	}

	// format time
	toLocation, err := time.LoadLocation(extra.ToLocation)
	if err != nil {
		return errors.WithMessagef(err, "load location %s from field %s error", extra.FromLocation, t.key)
	}
	timestamp = timestamp.In(toLocation)

	switch extra.ToLayout {
	case LayoutUnix:
		eventops.Set(e, t.key, timestamp.Unix())

	case LayoutUnixMs:
		eventops.Set(e, t.key, timestamp.UnixMilli())

	default:
		eventops.Set(e, t.key, timestamp.Format(extra.ToLayout))
	}

	return nil
}
