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

type UnderRootProcessor struct {
	config *UnderRootConfig
}

type UnderRootConfig struct {
	Target []string `yaml:"target,omitempty"`
}

func init() {
	register("underRoot", func() Processor {
		return NewUnderRootProcessor()
	})
}

func NewUnderRootProcessor() *UnderRootProcessor {
	return &UnderRootProcessor{
		config: &UnderRootConfig{},
	}
}

func (d *UnderRootProcessor) Config() interface{} {
	return d.config
}

func (d *UnderRootProcessor) Process(jsonObj *simplejson.Json) {
	if d.config == nil {
		return
	}
	for _, t := range d.config.Target {
		upperPaths, key := util.GetQueryUpperPaths(t)
		upperVal := jsonObj.GetPath(upperPaths...)
		val := upperVal.Get(key)
		if valMap, err := val.Map(); err == nil {
			for k, v := range valMap {
				jsonObj.Set(k, v)
			}
		} else {
			jsonObj.Set(key, val)
		}
		upperVal.Del(key)
	}
}
