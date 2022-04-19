package interceptor

import (
	"testing"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/interceptor/normalize"
)

func BenchmarkSplitProcess(b *testing.B) {

	log.InitDefaultLogger()

	tests := []struct {
		name          string
		initProcessor func() normalize.Processor
		event         api.Event
	}{
		{
			name: "splitBody",
			initProcessor: func() normalize.Processor {
				proc := normalize.NewSplitProcessor()
				conf := proc.Config().(*normalize.SplitConfig)
				conf.Target = "body"
				conf.Separator = " "
				conf.Max = -1
				conf.Keys = []string{"ip", "time", "method", "status", "bytes"}
				return proc
			},
			event: event.NewEvent(map[string]interface{}{"a": "b"}, []byte(`10.244.0.1 [13/Dec/2021:12:40:48] GET 404 683`)),
		},
		{
			name: "splitHeader",
			initProcessor: func() normalize.Processor {
				proc := normalize.NewSplitProcessor()
				conf := proc.Config().(*normalize.SplitConfig)
				conf.Target = "content"
				conf.Separator = " "
				conf.Max = -1
				conf.Keys = []string{"ip", "time", "method", "status", "bytes"}
				return proc
			},
			event: event.NewEvent(map[string]interface{}{"content": `10.244.0.1 [13/Dec/2021:12:40:48] GET 404 683`}, []byte("")),
		},
		{
			name: "splitHeaderByPath",
			initProcessor: func() normalize.Processor {
				proc := normalize.NewSplitProcessor()
				conf := proc.Config().(*normalize.SplitConfig)
				conf.Target = "content.msg"
				conf.Separator = " "
				conf.Max = -1
				conf.Keys = []string{"ip", "time", "method", "status", "bytes"}
				return proc
			},
			event: event.NewEvent(map[string]interface{}{"content": map[string]interface{}{"msg": `10.244.0.1 [13/Dec/2021:12:40:48] GET 404 683`}}, []byte("")),
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
