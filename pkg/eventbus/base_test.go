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

package eventbus

import "testing"

func TestLogAlertData_Fingerprint(t *testing.T) {
	type fields struct {
		Labels map[string]string
	}
	tests := []struct {
		name    string
		fields1 fields
		fields2 fields
		equal   bool
	}{
		{
			name: "equal disorder",
			fields1: fields{
				Labels: map[string]string{
					"source": "forTest1",
					"host":   "127.0.0.1",
				},
			},
			fields2: fields{
				Labels: map[string]string{
					"host":   "127.0.0.1",
					"source": "forTest1",
				},
			},
			equal: true,
		},
		{
			name: "not equal-1",
			fields1: fields{
				Labels: map[string]string{
					"host":   "127.0.0.1",
					"source": "forTest1",
				},
			},
			fields2: fields{
				Labels: map[string]string{
					"host":   "0.0.0.0",
					"source": "forTest1",
				},
			},
			equal: false,
		},
		{
			name: "not equal-2",
			fields1: fields{
				Labels: map[string]string{
					"host":   "127.0.0.1",
					"source": "forTest1",
				},
			},
			fields2: fields{
				Labels: map[string]string{
					"host":      "0.0.0.0",
					"source":    "forTest1",
					"namespace": "ns1",
				},
			},
			equal: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lad1 := &LogAlertData{
				Labels: tt.fields1.Labels,
			}

			lad2 := &LogAlertData{
				Labels: tt.fields2.Labels,
			}

			if (lad1.Fingerprint() == lad2.Fingerprint()) != tt.equal {
				t.Errorf("lad1 Fingerprint() = %v, lad2 Fingerprint() = %v", lad1.Fingerprint(), lad2.Fingerprint())
			}

		})
	}
}
