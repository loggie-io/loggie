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

package transformer

import (
	"github.com/bitly/go-simplejson"
	"loggie.io/loggie/pkg/util"
)

type MoveProcessor struct {
	config *MoveConfig
}

type MoveConfig struct {
	Target []Convert `yaml:"target,omitempty"`
}

func init() {
	register("move", func() Processor {
		return NewMoveProcessor()
	})
}

func NewMoveProcessor() *MoveProcessor {
	return &MoveProcessor{
		config: &MoveConfig{},
	}
}

func (d *MoveProcessor) Config() interface{} {
	return d.config
}

func (d *MoveProcessor) Process(jsonObj *simplejson.Json) {
	if d.config == nil {
		return
	}
	for _, t := range d.config.Target {
		from := t.From
		to := t.To

		upperPath, lastQuery := util.GetQueryUpperPaths(from)
		upperObj := jsonObj.GetPath(upperPath...)

		tmp := upperObj.Get(lastQuery)
		toPaths := util.GetQueryPaths(to)
		jsonObj.SetPath(toPaths, tmp)

		upperObj.Del(lastQuery)
	}
}
