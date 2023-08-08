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

package sonic

import (
	json "github.com/bytedance/sonic"
	jsoniter "github.com/json-iterator/go"
)

type Sonic struct {
}

func (s *Sonic) Marshal(v interface{}) ([]byte, error) {
	return json.ConfigFastest.Marshal(v)
}

func (s *Sonic) Unmarshal(data []byte, v interface{}) error {
	return json.ConfigFastest.Unmarshal(data, v)
}

func (s *Sonic) MarshalIndent(v interface{}, prefix, indent string) ([]byte, error) {
	// SIMD json marshal not support this feature
	return jsoniter.ConfigFastest.MarshalIndent(v, prefix, indent)
}

func (s *Sonic) MarshalToString(v interface{}) (string, error) {
	return json.ConfigFastest.MarshalToString(v)
}
