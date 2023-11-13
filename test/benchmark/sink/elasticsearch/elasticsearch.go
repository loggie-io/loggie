package main

import (
	"github.com/loggie-io/loggie/pkg/control"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/queue"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/eventbus/export/logger"
	"github.com/loggie-io/loggie/pkg/interceptor/maxbytes"
	"github.com/loggie-io/loggie/pkg/interceptor/metric"
	"github.com/loggie-io/loggie/pkg/interceptor/retry"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/queue/channel"
	"net/http"
	"time"

	_ "github.com/loggie-io/loggie/pkg/include"
)

const pipe1 = `
pipelines:
  - name: test
    sources:
      - type: dev
        name: test
        qps: 100
        byteSize: 10240
        eventsTotal: 10000
    sink:
      type: elasticsearch
      parallelism: 3
      hosts: ["localhost:9200"]
      index: "loggie-benchmark-${+YYYY.MM.DD}"
`

func main() {
	log.InitDefaultLogger()
	pipeline.SetDefaultConfigRaw(pipeline.Config{
		Queue: &queue.Config{
			Type: channel.Type,
		},
		Interceptors: []*interceptor.Config{
			{
				Type: metric.Type,
			},
			{
				Type: maxbytes.Type,
			},
			{
				Type: retry.Type,
			},
		},
	})

	eventbus.StartAndRun(eventbus.Config{
		LoggerConfig: logger.Config{
			Enabled: true,
			Period:  5 * time.Second,
			Pretty:  false,
		},
		ListenerConfigs: map[string]cfg.CommonCfg{
			"sink": map[string]interface{}{
				"period": 5 * time.Second,
			},
			"sys": map[string]interface{}{
				"period": 5 * time.Second,
			},
		},
	})

	pipecfgs := &control.PipelineConfig{}
	if err := cfg.UnPackFromRaw([]byte(pipe1), pipecfgs).Defaults().Validate().Do(); err != nil {
		log.Panic("pipeline configs invalid: %v", err)
	}

	controller := control.NewController()
	controller.Start(pipecfgs)

	if err := http.ListenAndServe(":9196", nil); err != nil {
		log.Fatal("http listen and serve err: %v", err)
	}
}
