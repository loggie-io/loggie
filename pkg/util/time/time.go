/*
Copyright 2021 Loggie Authors

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

package time

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
)

const (
	year      = "yyyy"
	stdYear   = "2006"
	month     = "MM"
	stdMonth  = "01"
	day       = "dd"
	stdDay    = "02"
	hour      = "hh"
	stdHour   = "15"
	min       = "mm"
	stdMin    = "04"
	second    = "ss"
	stdSecond = "05"
	timezone  = "zz"
	stdTZ     = "-07"
	week      = "ww"
)

func TimeFormatNow(pattern string) string {
	replacer := strings.NewReplacer(year, stdYear, month, stdMonth, day, stdDay, hour, stdHour, min, stdMin, second, stdSecond, timezone, stdTZ)
	layout := replacer.Replace(pattern)

	t := time.Now().Local()
	_, weekNow := t.ISOWeek()
	weekString := strconv.Itoa(weekNow)
	weekReplacer := strings.NewReplacer(week, weekString)

	return weekReplacer.Replace(t.Format(layout))
}

func UnixMilli(t time.Time) int64 {
	return t.Unix()*1e3 + int64(t.Nanosecond())/1e6
}

type Duration struct {
	duration model.Duration
}

// MarshalYAML implements the yaml.Marshaler interface.
func (d Duration) MarshalYAML() (interface{}, error) {
	return d.String(), nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	dur, err := model.ParseDuration(s)
	if err != nil {
		return err
	}
	d.duration = dur
	return nil
}

func (d *Duration) String() string {
	return d.duration.String()
}

// Duration return time.duration struct
func (d *Duration) Duration() time.Duration {
	return time.Duration(d.duration)
}
