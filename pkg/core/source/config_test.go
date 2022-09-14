package source

import (
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/source/codec"
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
              type: file
              paths:
              - /var/log/*.log
              ignoreOlder: 2d
              codec:
                type: json
                bodyFields: log
            `),
			want: Config{
				Name: "bar",
				Type: "file",
				Properties: cfg.CommonCfg{
					"paths":       []interface{}{"/var/log/*.log"},
					"ignoreOlder": "2d",
				},
				Codec: &codec.Config{
					Type: "json",
					CommonCfg: cfg.CommonCfg{
						"bodyFields": "log",
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
				Fields: map[string]interface{}{
					"a": "b",
				},
				Codec: &codec.Config{
					Type: "json",
					CommonCfg: cfg.CommonCfg{
						"bodyFields": "log",
					},
				},
				Name: "test",
				Type: "file",
				Properties: cfg.CommonCfg{
					"paths": []interface{}{"/path1"},
				},
			},
			want: `
name: test
type: file
paths: ["/path1"]
fields:
  a: b
codec:
  type: json
  bodyFields: log
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

func TestMergeSourceList(t *testing.T) {
	type args struct {
		base []*Config
		from []*Config
	}
	defaultValue := false
	tests := []struct {
		name string
		args args
		want []*Config
	}{
		{
			name: "normal",
			args: args{
				base: []*Config{
					{
						FieldsUnderRoot: &defaultValue,
						Type:            "file",
						Name:            "test",
						Properties: cfg.CommonCfg{
							"paths": []string{
								"/log1",
							},
						},
						Fields: map[string]interface{}{
							"a": "b",
						},
					},
				},
				from: []*Config{
					{
						FieldsUnderRoot: &defaultValue,
						Type:            "file",
						Properties: cfg.CommonCfg{
							"ignoreOlder": "10d",
						},
						Fields: map[string]interface{}{
							"a": "c",
							"d": "e",
						},
						Codec: &codec.Config{
							Type: "json",
							CommonCfg: cfg.CommonCfg{
								"prune": true,
							},
						},
					},
				},
			},
			want: []*Config{
				{
					FieldsUnderRoot: &defaultValue,
					Type:            "file",
					Name:            "test",
					Properties: cfg.CommonCfg{
						"paths": []string{
							"/log1",
						},
						"ignoreOlder": "10d",
					},
					Fields: map[string]interface{}{
						"a": "b",
						"d": "e",
					},
					Codec: &codec.Config{
						Type: "json",
						CommonCfg: cfg.CommonCfg{
							"prune": true,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MergeSourceList(tt.args.base, tt.args.from)
			assert.Equal(t, tt.want, got)
		})
	}
}
