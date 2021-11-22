package backoff

import (
	"time"
)

type Backoff interface {
	Next() time.Duration
	Reset()
}
