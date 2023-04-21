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

package limit

import (
	"runtime"
	"time"
)

type clockDescriptor struct {
	clock Clock
}

func NewHighPrecisionClockDescriptor(clock Clock) Clock {
	return &clockDescriptor{clock: clock}
}

func (c *clockDescriptor) Sleep(d time.Duration) {
	// Sleeps less than 10ms use long polling instead to improve precision
	if d < time.Millisecond*10 {
		start := time.Now()
		needYield := d >= time.Millisecond*5
		for time.Since(start) < d {
			if needYield {
				runtime.Gosched()
			}
			// do nothing
		}
		return
	}
	c.clock.Sleep(d)
}

func (c *clockDescriptor) Now() time.Time {
	return c.clock.Now()
}
