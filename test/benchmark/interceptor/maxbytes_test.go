package interceptor

import (
	"testing"

	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/interceptor/maxbytes"
)

func BenchmarkMaxBytes(b *testing.B) {

	log.InitDefaultLogger()

	invoker := source.NewFakeInvoker()

	tests := []struct {
		name            string
		mockInterceptor func() source.Interceptor
		mockInvocation  func() source.Invocation
	}{
		{
			name: "normal",
			mockInterceptor: func() source.Interceptor {
				icp := maxbytes.NewInterceptor()
				conf := icp.Config().(*maxbytes.Config)
				conf.MaxBytes = 131072
				return icp
			},
			mockInvocation: func() source.Invocation {
				return source.Invocation{
					Event: event.NewEvent(map[string]interface{}{}, []byte(`{"ip":"10.244.0.1","time":"[13/Dec/2021:12:40:48]","method":"GET","status":404,"bytes":683}`)),
				}
			},
		},
		{
			name: "split",
			mockInterceptor: func() source.Interceptor {
				icp := maxbytes.NewInterceptor()
				conf := icp.Config().(*maxbytes.Config)
				conf.MaxBytes = 13
				return icp
			},
			mockInvocation: func() source.Invocation {
				return source.Invocation{
					Event: event.NewEvent(map[string]interface{}{}, []byte(`{"ip":"10.244.0.1","time":"[13/Dec/2021:12:40:48]","method":"GET","status":404,"bytes":683}`)),
				}
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			icp := tt.mockInterceptor()

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				icp.Intercept(invoker, tt.mockInvocation())
			}
		})
	}

}
