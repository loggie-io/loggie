package event

import (
	"fmt"
	"strings"
)

//go:generate msgp

type DefaultEventS struct {
	H map[string]interface{} `json:"header" msg:"header"`
	B []byte                 `json:"body" msg:"body"`
	M *DefaultMeta           `json:"meta" msg:"meta"`
}

type DefaultMeta struct {
	Properties map[string]interface{} `json:"properties" msg:"properties"`
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
