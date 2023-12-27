package ack

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
)

const AckMarkName = "streamAck"

type Mark interface {
	GetSourceName() string
	GetName() string
	GetAckId() string
	GetProgress() storage.CollectProgress
	GetEpoch() *pipeline.Epoch
}

func GetAckMark(e api.Event) Mark {
	if e == nil {
		panic("event is nil")
	}
	state, _ := e.Meta().Get(AckMarkName)
	if state == nil {
		return nil
	}
	return state.(Mark)
}
