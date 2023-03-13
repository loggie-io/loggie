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

package hdfs

import "github.com/loggie-io/loggie/pkg/util/pattern"

type Config struct {
	NameNode []string `yaml:"nameNode,omitempty"`
	// BaseDirs is a collection of directories that specify which directories to
	// write to, usually used when mounting a disk, and can be empty
	BaseDirs []string `yaml:"baseDirs,omitempty"`
	// DirHashKey
	DirHashKey string `yaml:"dirHashKey,omitempty"`
	// Filename is the file to write logs to.  Backup log files will be retained
	// in the same directory.
	Filename string `yaml:"filename,omitempty" validate:"required"`
	// LocalTime determines if the time used for formatting the timestamps in
	// backup files is the computer's local time.  The default is to use UTC
	// time.
	LocalTime bool `yaml:"localTime,omitempty"`
}

func (c *Config) Validate() error {
	if c.DirHashKey != "" {
		if err := pattern.Validate(c.DirHashKey); err != nil {
			return err
		}
	}

	if err := pattern.Validate(c.Filename); err != nil {
		return err
	}

	return nil
}
