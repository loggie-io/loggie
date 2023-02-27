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

package dev

import "time"

type Config struct {
	// in general, we use codec.PrintEvents instead.
	PrintEvents bool `yaml:"printEvents,omitempty" default:"false"`
	// Within this period, only one log event is printed for troubleshooting.
	PrintEventsInterval time.Duration `yaml:"printEventsInterval,omitempty"`

	PrintMetrics    bool          `yaml:"printMetrics,omitempty"`
	MetricsInterval time.Duration `yaml:"printMetricsInterval,omitempty" default:"1s"`
}
