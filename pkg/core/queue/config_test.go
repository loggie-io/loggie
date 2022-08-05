package queue

import (
	"github.com/loggie-io/loggie/pkg/core/cfg"
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
              type: channel
              batchAggTimeout: 1s
              batchSize: 1024
            `),
			want: Config{
				Name: "bar",
				Type: "channel",
				Properties: cfg.CommonCfg{
					"batchAggTimeout": "1s",
				},
				BatchSize: 1024,
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
				Name: "bar",
				Type: "channel",
				Properties: cfg.CommonCfg{
					"batchAggTimeout": "1s",
				},
				BatchSize: 1024,
			},
			want: `
              name: bar
              type: channel
              batchAggTimeout: 1s
              batchSize: 1024
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
					Type:      "channel",
					BatchSize: 1024,
				},
				from: &Config{
					Type:      "channel",
					BatchSize: 2048,
				},
			},
			want: &Config{
				Type:      "channel",
				BatchSize: 1024,
			},
		},
		{
			name: "override",
			args: args{
				base: &Config{
					Type: "channel",
				},
				from: &Config{
					Type:      "channel",
					BatchSize: 2048,
				},
			},
			want: &Config{
				Type:      "channel",
				BatchSize: 2048,
			},
		},
		{
			name: "type not equal",
			args: args{
				base: &Config{
					Type: "channel",
				},
				from: &Config{
					Type:      "memory",
					BatchSize: 2048,
				},
			},
			want: &Config{
				Type: "channel",
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
