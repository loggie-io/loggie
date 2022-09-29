package configuration

import (
	"github.com/loggie-io/loggie/pkg/control"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/concurrency"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/core/queue"
	"github.com/loggie-io/loggie/pkg/core/sink"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/core/sysconfig"
	_ "github.com/loggie-io/loggie/pkg/include"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"github.com/loggie-io/loggie/pkg/util/yaml"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMergeDefault(t *testing.T) {
	sys := `
loggie:
  defaults:
    sink:
      type: dev
      printEvents: true
      codec:
        type: json
        pretty: true
    sources:
      - type: file
        fields:
          c: d
        watcher:
          cleanFiles:
            maxHistory: 1
`

	raw := `
pipelines:
 - name: p1
   sources:
     - type: file
       name: demo
       paths:
         - "/tmp/log/*.log"
       fields:
         a: b
   interceptors:
   - type: normalize
`

	sysconf := &sysconfig.Config{}
	err := cfg.UnPackFromRaw([]byte(sys), sysconf).Defaults().Validate().Do()
	assert.NoError(t, err)

	pipe := &control.PipelineConfig{}
	err = cfg.UnPackFromRaw([]byte(raw), pipe).Defaults().Validate().Do()
	assert.NoError(t, err)

	want := &control.PipelineConfig{
		Pipelines: []pipeline.Config{
			{
				Name:             "p1",
				CleanDataTimeout: 5 * time.Second,
				Sources: []*source.Config{
					{
						Name: "demo",
						Type: "file",
						Properties: cfg.CommonCfg{
							"paths": []interface{}{"/tmp/log/*.log"},
							"watcher": map[interface{}]interface{}{
								"cleanFiles": map[interface{}]interface{}{
									"maxHistory": 1,
								},
							},
						},
						FieldsUnderKey: "fields",
						Fields: map[string]interface{}{
							"a": "b",
							"c": "d",
						},
					},
				},
				Queue: &queue.Config{
					Type:      "channel",
					BatchSize: 2048,
				},
				Interceptors: []*interceptor.Config{
					{
						Type: "normalize",
					},
					{
						Type: "metric",
					},
					{
						Type: "maxbytes",
					},
					{
						Type: "retry",
					},
				},
				Sink: &sink.Config{
					Type: "dev",
					Properties: cfg.CommonCfg{
						"printEvents": true,
					},
					Codec: codec.Config{
						Type: "json",
						CommonCfg: cfg.CommonCfg{
							"pretty": true,
						},
					},
					Parallelism: 1,
					Concurrency: concurrency.Config{
						Enable: false,
						Goroutine: &concurrency.Goroutine{
							InitThreshold:    16,
							MaxGoroutine:     30,
							UnstableTolerate: 3,
							ChannelLenOfCap:  0.4,
						},
						Rtt: &concurrency.Rtt{
							BlockJudgeThreshold: 1.2,
							NewRttWeigh:         0.5,
						},
						Ratio: &concurrency.Ratio{
							Multi:             2,
							Linear:            2,
							LinearWhenBlocked: 4,
						},
						Duration: &concurrency.Duration{
							Unstable: 15,
							Stable:   30,
						},
					},
				},
			},
		},
	}

	assert.Equal(t, want, pipe)
}

func TestPipelineInvalid(t *testing.T) {
	sysconf := &sysconfig.Config{}
	err := cfg.UnPackFromRaw([]byte{}, sysconf).Defaults().Validate().Do()
	assert.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		containsErr string
	}{
		{
			name: "pipeline name required",
			input: `
pipelines:
 - sources:
     - type: file
       paths:
         - "/tmp/log/*.log"
   sink:
     type: dev
`,
			containsErr: "Key: 'PipelineConfig.Pipelines[0].Name' Error:Field validation for 'Name' failed on the 'required' tag",
		},
		{
			name: "pipeline duplicated",
			input: `
pipelines:
  - name: p1
    sources:
      - type: file
        name: demo
        paths:
          - "/tmp/log/a.log"
    sink:
      type: dev
  - name: p1
    sources:
      - type: file
        name: demo
        paths:
          - "/tmp/log/b.log"
    sink:
      type: dev
`,
			containsErr: "pipeline name is duplicated",
		},
		{
			name: "source name required",
			input: `
pipelines:
 - name: p1
   sources:
     - type: file
       paths:
         - "/tmp/log/*.log"
       fields:
         a: b
   sink:
     type: dev
`,
			containsErr: pipeline.ErrSourceNameRequired.Error(),
		},
		{
			name: "source name unique",
			input: `
pipelines:
 - name: p1
   sources:
     - type: file
       name: demo
       paths:
         - "/tmp/log/*.log"
     - type: file
       name: demo
       paths:
         - "/tmp/log/*.log"
   sink:
     type: dev
`,
			containsErr: "source name demo is duplicated",
		},
		{
			name: "filesource missing paths",
			input: `
pipelines:
 - name: p1
   sources:
     - type: file
       name: demo
   sink:
     type: dev
`,
			containsErr: "Field validation for 'Paths' failed on the 'required' tag",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipe := &control.PipelineConfig{}
			err = cfg.UnPackFromRaw([]byte(tt.input), pipe).Defaults().Validate().Do()
			assert.ErrorContains(t, err, tt.containsErr)
		})
	}
}

func TestMarshal_PipelineConfig(t *testing.T) {
	tests := []struct {
		name string
		args control.PipelineConfig
		want string
	}{
		{
			name: "ok",
			args: control.PipelineConfig{
				Pipelines: []pipeline.Config{
					{
						Name: "local",
						Sources: []*source.Config{
							{
								Name: "demo",
								Type: "file",
								Properties: cfg.CommonCfg{
									"paths": []interface{}{"/tmp/log/*.log"},
								},
								Fields: map[string]interface{}{
									"topic": "a",
								},
							},
						},
						Sink: &sink.Config{
							Type: "dev",
							Properties: cfg.CommonCfg{
								"printEvents": true,
							},
							Codec: codec.Config{
								Type: "json",
								CommonCfg: cfg.CommonCfg{
									"pretty": true,
								},
							},
						},
					},
				},
			},
			want: `
pipelines:
  - name: local
    sources:
      - type: file
        name: demo
        paths:
          - /tmp/log/*.log
        fields:
          topic: "a"
    sink:
      type: dev
      printEvents: true
      codec:
        type: json
        pretty: true
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := yaml.Marshal(&tt.args)
			assert.NoError(t, err)
			assert.YAMLEq(t, tt.want, string(out))
		})
	}
}
