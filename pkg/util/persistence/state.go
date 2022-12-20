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
