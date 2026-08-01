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
	"strconv"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/util/eventops"
	timeutil "github.com/loggie-io/loggie/pkg/util/time"
	"github.com/pkg/errors"
)

const (
	TimestampOffsetName     = "timestampOffset"
	TimestampOffsetUsageMsg = "usage: timestampOffset(key)"
)

func init() {
	RegisterAction(TimestampOffsetName, func(args []string, extra cfg.CommonCfg) (Action, error) {
		return NewTimestampOffset(args, extra)
	})
}

// TimestampOffset converts a timestamp field and additionally writes offset-before and offset-after timestamps
// into separate configurable fields.
type TimestampOffset struct {
	key   string
	extra *timestampOffsetExtra
}

type timestampOffsetExtra struct {
	FromLayout       string `yaml:"fromLayout,omitempty" validate:"required"`
	FromLocation     string `yaml:"fromLocation,omitempty"`
	ToLayout         string `yaml:"toLayout,omitempty" validate:"required"`
	ToLocation       string `yaml:"toLocation,omitempty"`
	BeforeKey        string `yaml:"beforeKey,omitempty" validate:"required"`
	AfterKey         string `yaml:"afterKey,omitempty" validate:"required"`
	OffsetDuration   string `yaml:"offsetDuration,omitempty" validate:"required"`
	OffsetToLayout   string `yaml:"offsetToLayout,omitempty"`
	OffsetToLocation string `yaml:"offsetToLocation,omitempty"`
}

func NewTimestampOffset(args []string, extra cfg.CommonCfg) (*TimestampOffset, error) {
	aCount := len(args)
	if aCount != 1 {
		return nil, errors.Errorf("invalid args, %s", TimestampOffsetUsageMsg)
	}

	timeExtra := &timestampOffsetExtra{}
	if err := cfg.UnpackFromCommonCfg(extra, timeExtra).Defaults().Validate().Do(); err != nil {
		return nil, err
	}

	// Validate offsetDuration is parseable
	if _, err := time.ParseDuration(timeExtra.OffsetDuration); err != nil {
		return nil, errors.WithMessagef(err, "invalid offsetDuration %q", timeExtra.OffsetDuration)
	}

	return &TimestampOffset{
		key:   args[0],
		extra: timeExtra,
	}, nil
}

func (t *TimestampOffset) act(e api.Event) error {
	extra := t.extra
	fromVal := eventops.GetString(e, t.key)

	// ---- parse time ----
	timestamp, err := t.parseTime(fromVal, extra)
	if err != nil {
		return err
	}

	// ---- format and write back original ----
	out, err := timeutil.Format(timestamp, extra.ToLocation, extra.ToLayout)
	if err != nil {
		return err
	}
	eventops.Set(e, t.key, out)

	// ---- parse offset duration ----
	offsetDur, err := time.ParseDuration(extra.OffsetDuration)
	if err != nil {
		return errors.WithMessagef(err, "invalid offsetDuration %q", extra.OffsetDuration)
	}

	// resolve offset layout/location (fall back to main if not set)
	offsetLayout := extra.OffsetToLayout
	offsetLocation := extra.OffsetToLocation
	if offsetLayout == "" {
		offsetLayout = extra.ToLayout
	}
	if offsetLocation == "" {
		offsetLocation = extra.ToLocation
	}

	// ---- write before timestamp ----
	beforeTime := timestamp.Add(-offsetDur)
	beforeOut, err := timeutil.Format(beforeTime, offsetLocation, offsetLayout)
	if err != nil {
		return err
	}
	eventops.Set(e, extra.BeforeKey, beforeOut)

	// ---- write after timestamp ----
	afterTime := timestamp.Add(offsetDur)
	afterOut, err := timeutil.Format(afterTime, offsetLocation, offsetLayout)
	if err != nil {
		return err
	}
	eventops.Set(e, extra.AfterKey, afterOut)

	return nil
}

// parseTime parses the string value into time.Time based on fromLayout and fromLocation.
func (t *TimestampOffset) parseTime(fromVal string, extra *timestampOffsetExtra) (time.Time, error) {
	switch extra.FromLayout {
	case LayoutUnix:
		i, err := strconv.ParseInt(fromVal, 10, 64)
		if err != nil {
			return time.Time{}, errors.WithMessagef(err, "parse unix timestamp from field %q with value %q error", t.key, fromVal)
		}
		return time.Unix(i, 0), nil

	case LayoutUnixMs:
		i, err := strconv.ParseInt(fromVal, 10, 64)
		if err != nil {
			return time.Time{}, errors.WithMessagef(err, "parse unix_ms timestamp from field %q with value %q error", t.key, fromVal)
		}
		return time.UnixMilli(i), nil

	default:
		fromLocation, err := time.LoadLocation(extra.FromLocation)
		if err != nil {
			return time.Time{}, errors.WithMessagef(err, "load location %s from field %s error", extra.FromLocation, t.key)
		}
		fromTime, err := time.ParseInLocation(extra.FromLayout, fromVal, fromLocation)
		if err != nil {
			return time.Time{}, errors.WithMessagef(err, "parse timestamp with layout %q from field %q with value %q error", extra.FromLayout, t.key, fromVal)
		}
		return fromTime, nil
	}
}
