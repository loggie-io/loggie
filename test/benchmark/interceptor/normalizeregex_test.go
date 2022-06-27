package interceptor

import (
	"testing"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/interceptor/normalize"
)

func BenchmarkRegexProcess(b *testing.B) {

	log.InitDefaultLogger()

	interceptor := &normalize.Interceptor{}

	tests := []struct {
		name          string
		initProcessor func() normalize.Processor
		event         api.Event
	}{
		{
			name: "regexBody",
			initProcessor: func() normalize.Processor {
				proc := normalize.NewRegexProcessor()
				conf := proc.Config().(*normalize.RegexConfig)
				conf.Target = "body"
				conf.Pattern = `(?<ip>\S+) (?<id>\S+) (?<u>\S+) (?<time>\[.*?\]) (?<url>\".*?\") (?<status>\S+) (?<size>\S+)`
				conf.UnderRoot = true
				proc.Init(interceptor)

				return proc
			},
			event: event.NewEvent(map[string]interface{}{}, []byte(`10.244.0.1 - - [13/Dec/2021:12:40:48 +0000] "GET / HTTP/1.1" 404 683`)),
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
