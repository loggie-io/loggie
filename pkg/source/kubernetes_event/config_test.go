package kubernetes_event

import (
	"testing"
	"time"

	"github.com/loggie-io/loggie/pkg/util/yaml"
	"github.com/stretchr/testify/assert"
)

func Test_Time(t *testing.T) {
	var c Config
	startTime := time.Now()
	yaml.Unmarshal([]byte("latestEventsPreviousTime: 10s"), &c)
	nowTime := startTime.Add(-c.LatestEventsPreviousTime)
	seconds := startTime.Sub(nowTime).Seconds()
	assert.Equal(t, seconds, 10.0)
}
