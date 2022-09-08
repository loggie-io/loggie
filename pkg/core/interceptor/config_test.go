package interceptor

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
              type: maxbytes
              maxBytes: 1000
            `),
			want: Config{
				Name: "bar",
				Type: "maxbytes",
				Properties: cfg.CommonCfg{
					"maxBytes": 1000,
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
				Name: "bar",
				Type: "maxbytes",
				Properties: cfg.CommonCfg{
					"maxBytes": 1000,
				},
			},
			want: `
              name: bar
              type: maxbytes
              maxBytes: 1000
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

func TestMergeInterceptorList(t *testing.T) {
	type args struct {
		base []*Config
		from []*Config
	}
	tests := []struct {
		name string
		args args
		want []*Config
	}{
		{
			name: "normal ok",
			args: args{
				base: []*Config{
					{
						Type: "normalize",
						Properties: cfg.CommonCfg{
							"a": "b",
						},
					},
					{
						Type: "maxbytes",
					},
				},
				from: []*Config{
					{
						Type: "normalize",
						Properties: cfg.CommonCfg{
							"a": "c",
							"d": "e",
						},
					},
					{
						Type: "cost",
					},
				},
			},
			want: []*Config{
				{
					Type: "normalize",
					Properties: cfg.CommonCfg{
						"a": "b",
						"d": "e",
					},
				},
				{
					Type: "maxbytes",
				},
				{
					Type: "cost",
				},
			},
		},
		{
			name: "merge when name is different",
			args: args{
				base: []*Config{
					{
						Type: "normalize",
						Name: "foo",
						Properties: cfg.CommonCfg{
							"a": "b",
						},
					},
				},
				from: []*Config{
					{
						Type: "normalize",
						Name: "bar",
						Properties: cfg.CommonCfg{
							"a": "c",
							"d": "e",
						},
					},
				},
			},
			want: []*Config{
				{
					Type: "normalize",
					Name: "foo",
					Properties: cfg.CommonCfg{
						"a": "b",
					},
				},
				{
					Type: "normalize",
					Name: "bar",
					Properties: cfg.CommonCfg{
						"a": "c",
						"d": "e",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MergeInterceptorList(tt.args.base, tt.args.from)
			assert.Equal(t, tt.want, got)
		})
	}
}
