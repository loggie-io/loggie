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
	"time"

	"github.com/andres-erbsen/clock"
)

// Limiter is used to rate-limit some process, possibly across goroutines.
// The process is expected to call Take() before every iteration, which
// may block to throttle the goroutine.
type Limiter interface {
	// Take should block to make sure that the RPS is met.
	Take() time.Time
}

// Clock is the minimum necessary interface to instantiate a rate limiter with
// a clock or mock clock, compatible with clocks created using
// github.com/andres-erbsen/clock.
type Clock interface {
	Now() time.Time
	Sleep(time.Duration)
}

// config configures a limiter.
type config struct {
	clock         Clock
	maxSlack      time.Duration
	per           time.Duration
	lock          bool
	highPrecision bool
}

// Option configures a Limiter.
type Option interface {
	apply(*config)
}

type clockOption struct {
	clock Clock
}

func (o clockOption) apply(c *config) {
	c.clock = o.clock
}

// WithClock returns an option for ratelimit.New that provides an alternate
// Clock implementation, typically a mock Clock for testing.
func WithClock(clock Clock) Option {
	return clockOption{clock: clock}
}

type unLockOption struct {
}

func (uo unLockOption) apply(c *config) {
	c.lock = false
}

func WithoutLock() Option {
	return unLockOption{}
}

type highPrecisionOption struct {
}

func (hpo highPrecisionOption) apply(c *config) {
	c.highPrecision = true
	c.clock = NewHighPrecisionClockDescriptor(c.clock)
}

func WithHighPrecision() Option {
	return highPrecisionOption{}
}

type slackOption int

func (o slackOption) apply(c *config) {
	c.maxSlack = time.Duration(o)
}

// WithoutSlack is an Option for ratelimit.New that initializes the limiter
// without any initial tolerance for bursts of traffic.
var WithoutSlack Option = slackOption(0)

type perOption time.Duration

func (p perOption) apply(c *config) {
	c.per = time.Duration(p)
}

// Per allows configuring limits for different time windows.
//
// The default window is one second, so New(100) produces a one hundred per
// second (100 Hz) rate limiter.
//
// New(2, Per(60*time.Second)) creates a 2 per minute rate limiter.
func Per(per time.Duration) Option {
	return perOption(per)
}

type unlimited struct{}

// NewUnlimited returns a RateLimiter that is not limited.
func NewUnlimited() Limiter {
	return unlimited{}
}

func (unlimited) Take() time.Time {
	return time.Now()
}

// buildConfig combines defaults with options.
func buildConfig(opts []Option) config {
	c := config{
		clock:    clock.New(),
		maxSlack: 10,
		per:      time.Second,
		lock:     true,
	}

	for _, opt := range opts {
		opt.apply(&c)
	}
	return c
}

type unsafeLimiter struct {
	last       time.Time
	sleepFor   time.Duration
	perRequest time.Duration
	maxSlack   time.Duration
	clock      Clock
}

// newUnsafeBased returns a new limiter that thread(goroutine) not safe.
func newUnsafeBased(rate int, opts ...Option) *unsafeLimiter {
	config := buildConfig(opts)
	l := &unsafeLimiter{
		perRequest: config.per / time.Duration(rate),
		maxSlack:   -1 * config.maxSlack * time.Second / time.Duration(rate),
		clock:      config.clock,
	}
	return l
}

// Take blocks to ensure that the time spent between multiple
// Take calls is on average time.Second/rate.
func (t *unsafeLimiter) Take() time.Time {
	now := t.clock.Now()

	// If this is our first request, then we allow it.
	if t.last.IsZero() {
		t.last = now
		return t.last
	}

	// sleepFor calculates how much time we should sleep based on
	// the perRequest budget and how long the last request took.
	// Since the request may take longer than the budget, this number
	// can get negative, and is summed across requests.
	t.sleepFor += t.perRequest - now.Sub(t.last)

	// We shouldn't allow sleepFor to get too negative, since it would mean that
	// a service that slowed down a lot for a short period of time would get
	// a much higher RPS following that.
	if t.sleepFor < t.maxSlack {
		t.sleepFor = t.maxSlack
	}

	// If sleepFor is positive, then we should sleep now.
	if t.sleepFor > 0 {
		t.clock.Sleep(t.sleepFor)
		t.last = now.Add(t.sleepFor)
		t.sleepFor = 0
	} else {
		t.last = now
	}

	return t.last
}
