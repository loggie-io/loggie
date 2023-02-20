/*
Copyright 2022 Loggie Authors

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
	"time"

	"github.com/loggie-io/loggie/pkg/pipeline"
)

type State struct {
	Epoch        *pipeline.Epoch `json:"-"`
	PipelineName string          `json:"-"`
	SourceName   string          `json:"-"`
	Offset       int64           `json:"offset"`
	NextOffset   int64           `json:"nextOffset"`
	Filename     string          `json:"filename,omitempty"`
	CollectTime  time.Time       `json:"collectTime,omitempty"`
	ContentBytes int64           `json:"contentBytes"`
	JobUid       string          `json:"jobUid,omitempty"`
	JobIndex     uint32          `json:"-"`
	EventUid     string          `json:"-"`
	LineNumber   int64           `json:"lineNumber,omitempty"`
	Tags         string          `json:"tags,omitempty"`

	// for cache
	WatchUid string

	// JobFields from job
	JobFields map[string]interface{}
}

func (s *State) AppendTags(tag string) {
	if s.Tags == "" {
		s.Tags = tag
	} else {
		s.Tags = s.Tags + "," + tag
	}
}
