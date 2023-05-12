/*
Copyright 2023 Loggie Authors

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

package size

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var sizeReg = regexp.MustCompile(`^(\d+(\.\d+)?)\s*(?i:(KB|MB|GB|B))?$`)

type Size struct {
	Bytes int64
}

func (s *Size) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var sizeStr string
	if err := unmarshal(&sizeStr); err != nil {
		return err
	}

	size, err := parseSize(sizeStr)
	if err != nil {
		return err
	}

	if size < 0 {
		return fmt.Errorf("size can't be negative")
	}

	s.Bytes = size
	return nil
}

func (s Size) MarshalYAML() (interface{}, error) {
	var sizeStr string
	bytes := float64(s.Bytes)

	switch {
	case bytes >= 1024*1024*1024:
		sizeStr = strconv.FormatFloat(bytes/(1024*1024*1024), 'f', 2, 64) + "GB"
	case bytes >= 1024*1024:
		sizeStr = strconv.FormatFloat(bytes/(1024*1024), 'f', 2, 64) + "MB"
	case bytes >= 1024:
		sizeStr = strconv.FormatFloat(bytes/1024, 'f', 2, 64) + "KB"
	default:
		sizeStr = strconv.FormatInt(s.Bytes, 10) + "B"
	}

	return sizeStr, nil
}

func parseSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)

	match := sizeReg.FindStringSubmatch(sizeStr)
	if match == nil {
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}

	size, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}
	var unit string
	if len(match) > 3 && match[3] != "" {
		unit = strings.ToUpper(match[3])
	}

	// Here we do not distinguish between KiB and KB, and they are unified as KiB
	switch unit {
	case "KB", "K", "KiB":
		size *= 1024
	case "MB", "M", "MiB":
		size *= 1024 * 1024
	case "GB", "G", "GiB":
		size *= 1024 * 1024 * 1024
	case "", "B":
		// no-op
	default:
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}

	return int64(size), nil
}
