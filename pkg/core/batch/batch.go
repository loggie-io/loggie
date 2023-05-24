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
	"encoding/json"
	"github.com/loggie-io/loggie/pkg/core/event"
	"sync"

	"github.com/loggie-io/loggie/pkg/core/api"
)

type DefaultBatch struct {
	Content  []api.Event
	Metadata map[string]interface{}
}

func (db *DefaultBatch) ToS() *DefaultBatchS {
	var events []*event.DefaultEventS
	for _, c := range db.Content {
		e := &event.DefaultEventS{
			H: c.Header(),
			B: c.Body(),
			M: &event.DefaultMeta{
				Properties: c.Meta().GetAll(),
			},
		}

		events = append(events, e)
	}

	return &DefaultBatchS{
		Content:  events,
		Metadata: db.Metadata,
	}
}

func (db *DefaultBatch) Meta() map[string]interface{} {
	return db.Metadata
}

func (db *DefaultBatch) Events() []api.Event {
	return db.Content
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
		Content:  events,
		Metadata: make(map[string]interface{}),
	}
	return b
}

func NewBatch() *DefaultBatch {
	b := pool.Get().(*DefaultBatch)
	*b = DefaultBatch{
		Metadata: make(map[string]interface{}),
	}
	return b
}

func ReleaseBatch(b *DefaultBatch) {
	*b = DefaultBatch{} // clear batch
	pool.Put(b)
}

func (db *DefaultBatch) JsonMarshal() ([]byte, error) {
	out, err := json.Marshal(db)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func JsonUnmarshal(in []byte) (*DefaultBatch, error) {
	o := &DefaultBatchS{}
	err := json.Unmarshal(in, o)
	if err != nil {
		return nil, err
	}

	return batchSToBatch(o), nil
}

func (db *DefaultBatch) MsgPackMarshal() ([]byte, error) {
	b := db.ToS()
	// TODO zero-allocation marshaling
	return b.MarshalMsg(nil)
}

func MsgPackUnmarshal(in []byte) (*DefaultBatch, error) {
	o := &DefaultBatchS{}
	_, err := o.UnmarshalMsg(in)
	if err != nil {
		return nil, err
	}

	return batchSToBatch(o), nil
}

func batchSToBatch(o *DefaultBatchS) *DefaultBatch {
	b := NewBatch()
	for _, c := range o.Content {
		e := &event.DefaultEvent{
			H: c.H,
			B: c.B,
			M: c.M,
		}

		b.Content = append(b.Content, e)
	}
	b.Metadata = o.Metadata

	return b
}
