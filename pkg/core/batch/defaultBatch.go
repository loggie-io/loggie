package batch

import "github.com/loggie-io/loggie/pkg/core/event"

//go:generate msgp

type DefaultBatchS struct {
	Content  []*event.DefaultEventS `json:"content" msg:"content"`
	Metadata map[string]interface{} `json:"metadata" msg:"metadata"`
}
