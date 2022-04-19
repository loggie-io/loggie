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

package batch

import (
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
)

type DefaultBatch struct {
	Es        []api.Event
	startTime time.Time
	meta      map[string]interface{}
}

func (db *DefaultBatch) Meta() map[string]interface{} {
	return db.meta
}

func (db *DefaultBatch) Events() []api.Event {
	return db.Es
}

func (db *DefaultBatch) Release() {
	ReleaseBatch(db)
}

var pool = sync.Pool{
	New: func() interface{} {
		return &DefaultBatch{}
	},
}

func NewBatchWithEvents(events []api.Event) *DefaultBatch {
	b := pool.Get().(*DefaultBatch)
	*b = DefaultBatch{
		Es:        events,
		startTime: time.Now(),
		meta:      make(map[string]interface{}),
	}
	return b
}

func NewBatch() *DefaultBatch {
	b := pool.Get().(*DefaultBatch)
	*b = DefaultBatch{
		startTime: time.Now(),
		meta:      make(map[string]interface{}),
	}
	return b
}

func ReleaseBatch(b *DefaultBatch) {
	*b = DefaultBatch{} // clear batch
	pool.Put(b)
}
