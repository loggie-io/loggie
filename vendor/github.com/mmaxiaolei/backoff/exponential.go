package backoff

import (
	"math"
	"math/rand"
	"time"
)

const (
	defaultMinInterval  = 500 * time.Millisecond
	defaultMaxInterval  = 1 * time.Minute
	defaultFactor       = 1.5
	defaultJitterFactor = 0.5
)

type Exponential struct {
	minInterval  time.Duration
	maxInterval  time.Duration
	factor       float64
	jitterFactor float64 // 0<jitterFactor<1
	attempts     float64
}

type ExponentialOption func(exponential *Exponential)

func NewExponentialBackoff(opts ...ExponentialOption) *Exponential {
	e := &Exponential{
		minInterval:  defaultMinInterval,
		maxInterval:  defaultMaxInterval,
		factor:       defaultFactor,
		jitterFactor: defaultJitterFactor,
		attempts:     0,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

func WithMinInterval(duration time.Duration) ExponentialOption {
	return func(e *Exponential) {
		e.minInterval = duration
	}
}

func WithMaxInterval(duration time.Duration) ExponentialOption {
	return func(e *Exponential) {
		e.maxInterval = duration
	}
}

func WithFactor(factor float64) ExponentialOption {
	return func(e *Exponential) {
		e.factor = factor
	}
}

func WithJitterFactor(jitterFactor float64) ExponentialOption {
	return func(e *Exponential) {
		if jitterFactor > 1 {
			jitterFactor = defaultJitterFactor
		}
		e.jitterFactor = jitterFactor
	}
}

func (e *Exponential) Reset() {
	e.attempts = 0
}

func (e *Exponential) Next() time.Duration {
	current := float64(e.minInterval) * math.Pow(e.factor, e.attempts)
	if current > float64(e.maxInterval) {
		current = float64(e.maxInterval)
	}
	if e.jitterFactor > 0 {
		j := e.jitterFactor * current
		min := current - j
		max := current + j
		current = min + rand.Float64()*(max-min+1)
	}
	e.attempts++
	return time.Duration(current)
}
