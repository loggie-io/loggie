package sink

import (
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"github.com/loggie-io/loggie/pkg/util/yaml"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConfig_Unmarshal(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want Config
	}{
		{
			name: "ok",
			in: []byte(`
              name: bar
              type: kafka
              brokers: ["127.0.0.1:8888"]
              topic: demo
              parallelism: 3
              codec:
                type: json
                pretty: true
            `),
			want: Config{
				Name: "bar",
				Type: "kafka",
				Properties: cfg.CommonCfg{
					"brokers": []interface{}{"127.0.0.1:8888"},
					"topic":   "demo",
				},
				Parallelism: 3,
				Codec: codec.Config{
					Type: "json",
					CommonCfg: cfg.CommonCfg{
						"pretty": true,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c Config
			err := yaml.Unmarshal(tt.in, &c)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, c)
		})
	}
}

func TestConfig_Marshal(t *testing.T) {

	tests := []struct {
		name string
		args Config
		want string
	}{
		{
			name: "ok",
			args: Config{
				Parallelism: 3,
				Name:        "test",
				Type:        "kafka",
				Properties: cfg.CommonCfg{
					"brokers": []interface{}{"127.0.0.1:8888"},
					"topic":   "demo",
				},
			},
			want: `
name: test
type: kafka
brokers: ["127.0.0.1:8888"]
topic: demo
parallelism: 3
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := yaml.Marshal(&tt.args)
			assert.NoError(t, err)
			assert.YAMLEq(t, tt.want, string(got))
		})
	}
}

func TestConfig_Merge(t *testing.T) {
	type args struct {
		base *Config
		from *Config
	}
	tests := []struct {
		name string
		args args
		want *Config
	}{
		{
			name: "common ok",
			args: args{
				base: &Config{
					Type:        "kafka",
					Parallelism: 5,
				},
				from: &Config{
					Type:        "kafka",
					Parallelism: 10,
					Codec: codec.Config{
						Type: "json",
						CommonCfg: cfg.CommonCfg{
							"pretty": true,
						},
					},
				},
			},
			want: &Config{
				Type:        "kafka",
				Parallelism: 5,
				Codec: codec.Config{
					Type: "json",
					CommonCfg: cfg.CommonCfg{
						"pretty": true,
					},
				},
			},
		},
		{
			name: "ok with codec",
			args: args{
				base: &Config{
					Type:        "kafka",
					Parallelism: 5,
					Codec: codec.Config{
						Type: "json",
						CommonCfg: cfg.CommonCfg{
							"pretty": false,
						},
					},
				},
				from: &Config{
					Type:        "kafka",
					Parallelism: 10,
					Codec: codec.Config{
						Type: "json",
						CommonCfg: cfg.CommonCfg{
							"pretty": true,
						},
					},
				},
			},
			want: &Config{
				Type:        "kafka",
				Parallelism: 5,
				Codec: codec.Config{
					Type: "json",
					CommonCfg: cfg.CommonCfg{
						"pretty": false,
					},
				},
			},
		},
		{
			name: "type not equal",
			args: args{
				base: &Config{
					Type: "kafka",
				},
				from: &Config{
					Type: "elasticsearch",
				},
			},
			want: &Config{
				Type: "kafka",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.args.base.Merge(tt.args.from)

			assert.Equal(t, tt.want, tt.args.base)
		})
	}
}
