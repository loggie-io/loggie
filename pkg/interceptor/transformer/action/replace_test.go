/*
Copyright 2023 Loggie Authors

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

package action

import (
	"testing"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/stretchr/testify/assert"
)

func TestReplace_act(t *testing.T) {
	log.InitDefaultLogger()
	type fields struct {
		key   string
		extra cfg.CommonCfg
	}
	type args struct {
		e api.Event
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   api.Event
	}{
		{
			name: "replace body",
			fields: fields{
				key: "body",
				extra: cfg.CommonCfg{
					"old": "c",
					"new": "C",
					"max": 3,
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{}, []byte(`2023/09/05 12:32:01 error zap.go:66: 192.168.0.1 54ce5d87-b94c-c40a-74a7-9cd375289334`)),
			},
			want: event.NewEvent(map[string]interface{}{
				"body": "2023/09/05 12:32:01 error zap.go:66: 192.168.0.1 54Ce5d87-b94C-C40a-74a7-9cd375289334",
			}, []byte(`2023/09/05 12:32:01 error zap.go:66: 192.168.0.1 54ce5d87-b94c-c40a-74a7-9cd375289334`)),
		},
		{
			name: "replace all",
			fields: fields{
				key: "body",
				extra: cfg.CommonCfg{
					"old": "3",
					"new": "W",
					"max": -1,
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{}, []byte(`2023/09/05 12:32:01 error zap.go:66: 192.168.0.1 54ce5d87-b94c-c40a-74a7-9cd375289334`)),
			},
			want: event.NewEvent(map[string]interface{}{
				"body": "202W/09/05 12:W2:01 error zap.go:66: 192.168.0.1 54ce5d87-b94c-c40a-74a7-9cdW75289WW4",
			}, []byte(`2023/09/05 12:32:01 error zap.go:66: 192.168.0.1 54ce5d87-b94c-c40a-74a7-9cd375289334`)),
		},
		{
			name: "replace all with Max default -1",
			fields: fields{
				key: "body",
				extra: cfg.CommonCfg{
					"old": "3",
					"new": "W",
				},
			},
			args: args{
				e: event.NewEvent(map[string]interface{}{}, []byte(`2023/09/05 12:32:01 error zap.go:66: 192.168.0.1 54ce5d87-b94c-c40a-74a7-9cd375289334`)),
			},
			want: event.NewEvent(map[string]interface{}{
				"body": "202W/09/05 12:W2:01 error zap.go:66: 192.168.0.1 54ce5d87-b94c-c40a-74a7-9cdW75289WW4",
			}, []byte(`2023/09/05 12:32:01 error zap.go:66: 192.168.0.1 54ce5d87-b94c-c40a-74a7-9cd375289334`)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := NewReplace([]string{tt.fields.key}, tt.fields.extra)
			err := r.act(tt.args.e)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, tt.args.e)

		})
	}
}
