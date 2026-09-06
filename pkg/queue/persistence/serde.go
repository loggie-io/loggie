/*
Copyright 2026 Loggie Authors

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

package persistence

import (
	"encoding/binary"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/util/json"
)

// walEntry is the on-disk representation of an event.
// Only header, body and the source name are persisted; other meta values
// (e.g. file collect states) hold runtime pointers that cannot survive a restart.
type walEntry struct {
	Header map[string]interface{} `json:"h,omitempty"`
	Body   []byte                 `json:"b,omitempty"`
	Source string                 `json:"s,omitempty"`
}

func marshalEvent(e api.Event) ([]byte, error) {
	entry := walEntry{
		Header: e.Header(),
		Body:   e.Body(),
	}
	if m := e.Meta(); m != nil {
		if v, ok := m.Get(event.SystemSourceKey); ok {
			if s, sok := v.(string); sok {
				entry.Source = s
			}
		}
	}
	return json.Marshal(entry)
}

// unmarshalEvent rebuilds an event replayed from disk after a restart.
// Replayed events do not belong to any running source, so their meta source
// is set to ReplaySourceName; the pipeline skips committing them upstream.
func unmarshalEvent(data []byte, pipelineName string) (api.Event, error) {
	entry := &walEntry{}
	if err := json.Unmarshal(data, entry); err != nil {
		return nil, err
	}

	meta := event.NewDefaultMeta()
	meta.Set(event.SystemPipelineKey, pipelineName)
	meta.Set(event.SystemSourceKey, ReplaySourceName)
	if entry.Source != "" {
		meta.Set(OriginSourceKey, entry.Source)
	}

	e := event.NewEvent(entry.Header, entry.Body)
	e.Fill(meta, e.Header(), entry.Body)
	return e, nil
}

func encodeKey(seq uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, seq)
	return key
}

func decodeKey(key []byte) (uint64, bool) {
	if len(key) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(key), true
}
