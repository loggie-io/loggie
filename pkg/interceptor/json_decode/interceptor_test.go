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

package json_decode

import (
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/event"
	"loggie.io/loggie/pkg/core/log"
	"strings"
	"testing"
)

func init() {
	log.InitLog()
}

var config = &Config{
	BodyKey:    "log",
	DropFields: []string{"stream", "time"},
}

var interceptor = &Interceptor{
	config: config,
}

func TestInterceptor_process(t *testing.T) {
	var header = make(map[string]interface{})
	var message = []byte("{\"log\":\"tmpfs on /proc/kcore type tmpfs (rw,nosuid,size=65536k,mode=755)\\r\\n\",\"stream\":\"stdout\",\"time\":\"2021-06-02T03:20:54.260571439Z\"}")

	type fields struct {
		name   string
		config *Config
	}
	type args struct {
		e api.Event
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "ok-1",
			fields: fields{
				config: config,
			},
			args: args{
				e: event.NewEvent(header, message),
			},
			want: []byte("tmpfs on /proc/kcore type tmpfs (rw,nosuid,size=65536k,mode=755)"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Interceptor{
				name:   tt.fields.name,
				config: tt.fields.config,
			}

			err := i.process(tt.args.e)
			if (err != nil) != tt.wantErr {
				t.Errorf("process() error = %v, wantErr %v", err, tt.wantErr)
			}

			if strings.TrimSpace(string(tt.args.e.Body())) != strings.TrimSpace(string(tt.want)) {
				t.Errorf("process() error = %v, want: %v", err, tt.want)
			}

		})
	}
}

func BenchmarkInterceptor_process(t *testing.B) {

	t.ResetTimer()
	t.StartTimer()
	for i := 0; i < t.N; i++ {

		var header = make(map[string]interface{})
		var message = []byte("{\"log\":\"tmpfs on /proc/kcore type tmpfs (rw,nosuid,size=65536k,mode=755)\\r\\n\",\"stream\":\"stdout\",\"time\":\"2021-06-02T03:20:54.260571439Z\"}")
		var ev = event.NewEvent(header, message)

		interceptor.process(ev)
	}

}
