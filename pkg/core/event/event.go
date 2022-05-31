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
	"fmt"
	"strings"

	"github.com/loggie-io/loggie/pkg/core/api"
)

const (
	SystemKeyPrefix  = "system"
	PrivateKeyPrefix = "@private"

	SystemPipelineKey    = SystemKeyPrefix + "PipelineName"
	SystemSourceKey      = SystemKeyPrefix + "SourceName"
	SystemProductTimeKey = SystemKeyPrefix + "ProductTime"

	Body = "body"
)

type DefaultMeta struct {
	Properties map[string]interface{} `json:"properties"`
}

func NewDefaultMeta() *DefaultMeta {
	return &DefaultMeta{
		Properties: make(map[string]interface{}),
	}
}

func (dm *DefaultMeta) Source() string {
	return dm.Properties[SystemSourceKey].(string)
}

func (dm *DefaultMeta) Get(key string) (value interface{}, exist bool) {
	value, exist = dm.Properties[key]
	return value, exist
}

func (dm *DefaultMeta) GetAll() map[string]interface{} {
	return dm.Properties
}

func (dm *DefaultMeta) Set(key string, value interface{}) {
	dm.Properties[key] = value
}

func (dm *DefaultMeta) String() string {
	var s strings.Builder
	s.WriteString("{")
	for k, v := range dm.Properties {
		s.WriteString(k)
		s.WriteString(":")
		s.WriteString(fmt.Sprintf("%#v", v))
		s.WriteString(",")
	}
	s.WriteString("}")
	return s.String()
}

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
	sb.WriteString("meta:")
	if de.M != nil {
		sb.WriteString(de.M.String())
	}
	sb.WriteString(";")
	sb.WriteString("header:")
	sb.WriteString("{")
	for k, v := range de.Header() {
		sb.WriteString(k)
		sb.WriteString(" : ")
		sb.WriteString(fmt.Sprintf("%+v", v))
		sb.WriteString(", ")
	}
	sb.WriteString("}; body:{")
	sb.WriteString(string(de.Body()))
	sb.WriteString("}")
	return sb.String()
}

type Factory func() api.Event
