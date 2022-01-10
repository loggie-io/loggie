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

package util

import (
	"strings"
	"time"
)

const (
	year     = "YYYY"
	stdYear  = "2006"
	month    = "MM"
	stdMonth = "01"
	day      = "DD"
	stdDay   = "02"
	hour     = "hh"
	stdHour  = "15"
)

func TimeFormatNow(pattern string) string {
	replacer := strings.NewReplacer(year, stdYear, month, stdMonth, day, stdDay, hour, stdHour)
	layout := replacer.Replace(pattern)
	return time.Now().Format(layout)
}

func UnixMilli(t time.Time) int64 {
	return t.Unix()*1e3 + int64(t.Nanosecond())/1e6
}
