package interceptor

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/interceptor/normalize"
	"testing"
)

func BenchmarkJSONDecodeProcess(b *testing.B) {

	log.InitDefaultLogger()

	tests := []struct {
		name          string
		initProcessor func() normalize.Processor
		event         api.Event
	}{
		{
			name: "jsonDecodeBody",
			initProcessor: func() normalize.Processor {
				proc := normalize.NewJsonDecodeProcessor()
				conf := proc.Config().(*normalize.JsonDecodeConfig)
				conf.Target = "body"

				return proc
			},
			event: event.NewEvent(map[string]interface{}{}, []byte(`{"ip":"10.244.0.1","time":"[13/Dec/2021:12:40:48]","method":"GET","status":404,"bytes":683}`)),
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			proc := tt.initProcessor()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				// process would override the header of event, but we can ignore it in the benchmark
				if err := proc.Process(tt.event); err != nil {
					b.Fatalf("process event failed: %v", err)
				}
			}
		})
	}

}
