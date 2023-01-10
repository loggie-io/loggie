package alertwebhook

import (
	"testing"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/stretchr/testify/assert"
)

func TestNewAlert(t *testing.T) {
	now := time.Now()
	text, _ := now.MarshalText()

	tests := []struct {
		name  string
		want  event.Alert
		event api.Event
	}{
		{
			name: "aa",
			event: &event.DefaultEvent{
				H: map[string]interface{}{
					"fields": map[string]interface{}{
						"topic": "loggie",
					},
					"reason": "reason",
				},
				B: []byte("message"),
				M: &event.DefaultMeta{Properties: map[string]interface{}{
					event.SystemPipelineKey:    "local",
					event.SystemSourceKey:      "demo",
					event.SystemProductTimeKey: now,
				}},
			},
			want: event.Alert{
				"body":   []string{"message"},
				"reason": "reason",
				"fields": map[string]interface{}{
					"topic": "loggie",
				},
				"_meta": map[string]interface{}{
					"pipelineName": "local",
					"sourceName":   "demo",
					"timestamp":    string(text),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alert := event.NewAlert(tt.event, 10)
			assert.EqualValues(t, tt.want, alert, "should be same")
		})
	}
}
