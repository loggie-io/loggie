/*
Copyright 2021 Loggie Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package control

import (
	"testing"

	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/source"
	_ "github.com/loggie-io/loggie/pkg/queue/memory"
	_ "github.com/loggie-io/loggie/pkg/sink/dev"
	_ "github.com/loggie-io/loggie/pkg/source/file"
	"github.com/pkg/errors"
)

var (
	pipelineNameUnique = `
pipelines:
  - name: xxx
    sources:
      - type: file
        name: "a"
        paths:
          - "/tmp/log/*.log"
    queue:
      type: memory
    sink:
      type: "dev"

  - name: xxx
    sources:
      - type: file
        name: "a"
        paths:
          - "/tmp/log/*.log"
    queue:
      type: memory
    sink:
      type: "dev"
`

	sourceNameRequired = `
pipelines:
  - name: p1
    sources:
      - type: file
        paths:
          - "/tmp/log/*.log"
    queue:
      type: memory
    sink:
      type: "dev"
`
)

func Test_defaultsAndValidate(t *testing.T) {
	type args struct {
		content []byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "pipeline_name_unique",
			args: args{
				content: []byte(pipelineNameUnique),
			},
			wantErr: ErrPipeNameUniq,
		},
		{
			name: "source_name_required",
			args: args{
				content: []byte(sourceNameRequired),
			},
			wantErr: source.ErrSourceNameRequired,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cfg.UnpackRawDefaultsAndValidate(tt.args.content, &PipelineRawConfig{})
			if err != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("defaultsAndValidate() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

		})
	}
}
