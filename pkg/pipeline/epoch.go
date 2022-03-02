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

package pipeline

import (
	"strconv"
	"strings"
	"time"
)

type Epoch struct {
	PipelineName string
	ReloadCount  int
	StartTime    time.Time
}

func NewEpoch(pipelineName string) *Epoch {
	return &Epoch{
		PipelineName: pipelineName,
		ReloadCount:  0,
		StartTime:    time.Now(),
	}
}

func (e *Epoch) IsEmpty() bool {
	return e == nil || e.StartTime.IsZero()
}

func (e *Epoch) Increase() {
	e.ReloadCount++
	e.StartTime = time.Now()
}

func (e *Epoch) Equal(ae *Epoch) bool {
	if e.PipelineName != ae.PipelineName {
		return false
	}
	if e.ReloadCount != ae.ReloadCount {
		return false
	}
	return e.StartTime.Equal(ae.StartTime)
}

func (e *Epoch) String() string {
	var es strings.Builder
	es.Grow(64)
	es.WriteString(e.PipelineName)
	es.WriteString(":")
	es.WriteString(strconv.Itoa(e.ReloadCount))
	es.WriteString(":")
	es.WriteString(e.StartTime.Format("2006-01-02 15:04:05"))
	return es.String()
}
