package batch

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDefaultBatch_JsonMarshalAndUnmarshal(t *testing.T) {
	type fields struct {
		b *DefaultBatch
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "ok",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &event.DefaultEvent{
				H: map[string]interface{}{
					"foo": "bar",
				},
				B: []byte("this is body"),
				M: event.NewDefaultMeta(),
			}
			e.M.Set("a", "b")

			var es []api.Event
			es = append(es, e)

			b := NewBatchWithEvents(es)
			got, err := b.JsonMarshal()
			assert.NoError(t, err)
			out, err := JsonUnmarshal(got)
			assert.NoError(t, err)
			assert.Equal(t, b, out)
		})
	}
}
