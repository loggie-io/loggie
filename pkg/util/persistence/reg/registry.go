/*
Copyright 2023 Loggie Authors

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

package reg

import (
	"bytes"
	"github.com/loggie-io/loggie/pkg/util/json"
)

type Registry struct {
	Id           int    `json:"id"`
	PipelineName string `json:"pipelineName"`
	SourceName   string `json:"sourceName"`
	Filename     string `json:"filename"`
	JobUid       string `json:"jobUid"`
	Offset       int64  `json:"offset"`
	CollectTime  string `json:"collectTime"`
	Version      string `json:"version"`
	LineNumber   int64  `json:"lineNumber"`
}

type RegistryList []Registry

func (r *Registry) Key() []byte {
	return GenKey(r.JobUid, r.SourceName, r.PipelineName)
}

func GenKey(jobuid, source, pipeline string) []byte {
	var bb bytes.Buffer
	bb.WriteString(jobuid)
	bb.WriteString("/")
	bb.WriteString(source)
	bb.WriteString("/")
	bb.WriteString(pipeline)
	return bb.Bytes()
}

func (r *Registry) Value() []byte {
	marshal, _ := json.Marshal(r)
	return marshal
}

func (r *Registry) CheckIntegrity() bool {
	return len(r.Filename) > 0 &&
		r.Offset > 0 &&
		len(r.CollectTime) > 0 &&
		len(r.Version) > 0 &&
		r.LineNumber > 0
}

func (r *Registry) Merge(registry Registry) {
	if len(registry.Filename) > 0 {
		r.Filename = registry.Filename
	}

	if registry.Offset > 0 {
		r.Offset = registry.Offset
	}

	if len(registry.CollectTime) > 0 {
		r.CollectTime = registry.CollectTime
	}

	if len(registry.Version) > 0 {
		r.Version = registry.Version
	}

	if registry.LineNumber > 0 {
		r.LineNumber = registry.LineNumber
	}
}
