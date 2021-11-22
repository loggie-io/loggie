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

type DropProcessor struct {
	config *DropConfig
}

type DropConfig struct {
	Target []string `yaml:"target,omitempty"`
}

func init() {
	register("drop", func() Processor {
		return NewDropProcessor()
	})
}

func NewDropProcessor() *DropProcessor {
	return &DropProcessor{
		config: &DropConfig{},
	}
}

func (d *DropProcessor) Config() interface{} {
	return d.config
}

func (d *DropProcessor) Process(jsonObj *simplejson.Json) {
	if d.config == nil {
		return
	}
	for _, t := range d.config.Target {
		upperPath, lastQuery := util.GetQueryUpperPaths(t)
		upperObj := jsonObj.GetPath(upperPath...)
		upperObj.Del(lastQuery)
	}
}
