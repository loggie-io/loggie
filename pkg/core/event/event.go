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

package event

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/pkg/errors"
	"strings"
)

const (
	SystemKeyPrefix  = "system"
	PrivateKeyPrefix = "@private"

	SystemPipelineKey    = SystemKeyPrefix + "PipelineName"
	SystemSourceKey      = SystemKeyPrefix + "SourceName"
	SystemProductTimeKey = SystemKeyPrefix + "ProductTime"

	Body = "body"
)

var ErrorDropEvent = errors.New("drop event")

type DefaultEvent struct {
	H map[string]interface{} `json:"header"`
	B []byte                 `json:"body"`
	M api.Meta               `json:"meta"`
}

func NewEvent(header map[string]interface{}, body []byte) *DefaultEvent {
	return &DefaultEvent{
		H: header,
		B: body,
	}
}

func newBlankEvent() *DefaultEvent {
	return &DefaultEvent{}
}

func (de *DefaultEvent) Meta() api.Meta {
	return de.M
}

func (de *DefaultEvent) Header() map[string]interface{} {
	if de.H == nil {
		de.H = make(map[string]interface{})
	}

	return de.H
}

func (de *DefaultEvent) Body() []byte {
	return de.B
}

func (de *DefaultEvent) Fill(meta api.Meta, header map[string]interface{}, body []byte) {
	de.H = header
	de.B = body
	de.M = meta
}

func (de *DefaultEvent) Release() {
	// clean event
	de.H = make(map[string]interface{})
	de.B = nil
	de.M = NewDefaultMeta()
}

func (de *DefaultEvent) String() string {
	var sb strings.Builder
	sb.WriteString(`header:`)
	header, _ := jsoniter.Marshal(de.Header())
	sb.Write(header)
	sb.WriteString(`, body:"`)
	sb.WriteString(string(de.Body()))
	sb.WriteString(`"`)
	return sb.String()
}

func (de *DefaultEvent) DeepCopy() api.Event {
	var e api.Event
	var meta api.Meta
	header := make(map[string]interface{})
	for k, v := range de.Header() {
		header[k] = v
	}
	meta = NewDefaultMeta()
	for k, v := range de.Meta().GetAll() {
		meta.Set(k, v)
	}

	body := make([]byte, len(de.Body()))
	copy(body, de.Body())

	e = NewEvent(header, nil)
	e.Fill(meta, header, body)
	return e
}
