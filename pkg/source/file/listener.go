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

package file

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/util/persistence"
)

const (
	ackListenerNamePrefix = "file-source-ack-listener"
)

type AckListener struct {
	sourceName      string
	ackChainHandler *AckChainHandler
}

func (al *AckListener) Name() string {
	return ackListenerNamePrefix + "-" + al.sourceName
}

func (al *AckListener) Stop() {

}

func (al *AckListener) BeforeQueueConvertBatch(events []api.Event) {
	//log.Info("append events len: %d", len(events))
	ss := make([]*persistence.State, 0, len(events))
	for _, e := range events {
		if al.sourceName == e.Meta().Source() {
			ss = append(ss, getState(e))
		}
	}
	if len(ss) > 0 {
		al.ackChainHandler.appendChan <- ss
	}
}
