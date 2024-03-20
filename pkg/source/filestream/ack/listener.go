package ack

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"go.uber.org/atomic"
)

const (
	ackListenerNamePrefix = "filestream-source-ack-listener"
)

type Listener struct {
	sourceName      string
	status          atomic.Bool
	ackChainHandler *ChainHandler
}

func NewListener(source string, ackChainHandler *ChainHandler) *Listener {
	return &Listener{
		sourceName:      source,
		ackChainHandler: ackChainHandler,
	}
}

func (al *Listener) Name() string {
	return ackListenerNamePrefix + "-" + al.sourceName
}

func (al *Listener) Stop() {

}

// BeforeQueueConvertBatch 发送sinker 之前，将要发送的内容标记入ack 协程
func (al *Listener) BeforeQueueConvertBatch(events []api.Event) {
	markList := make([]Mark, 0, len(events))
	for _, e := range events {
		if al.sourceName == e.Meta().Source() {
			mark := GetAckMark(e)
			if mark == nil {
				continue
			}

			markList = append(markList, mark)
		}
	}
	if len(markList) > 0 {
		al.ackChainHandler.appendChan <- markList
	}

}
