/*
Copyright 2022 Loggie Authors

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
	"strings"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
)

const (
	sourceName   = "sourceName"
	pipelineName = "pipelineName"
	timestamp    = "timestamp"

	meta = "_meta"

	DefaultAlertKey = "_defaultAlertKey"
	NoDataKey       = "NoDataAlert"
	Addition        = "additions"
	Fields          = "fields"
	ReasonKey       = "reason"

	AlertOriginDataKey = "Events"

	loggieError = "LoggieError"
)

type Alert map[string]interface{}

type AlertMap map[string][]Alert

type PackageAndSendAlerts func(alerts []Alert)

func NewAlert(e api.Event, lineLimit int) Alert {
	allMeta := e.Meta().GetAll()
	if allMeta == nil {
		allMeta = make(map[string]interface{})
	}

	if value, ok := allMeta[SystemSourceKey]; ok {
		allMeta[sourceName] = value
	}

	if value, ok := allMeta[SystemPipelineKey]; ok {
		allMeta[pipelineName] = value
	}

	if value, ok := allMeta[SystemProductTimeKey]; ok {
		t, valueToTime := value.(time.Time)
		if !valueToTime {
			allMeta[timestamp] = value
		} else {
			textTime, err := t.MarshalText()
			if err == nil {
				allMeta[timestamp] = string(textTime)
			} else {
				allMeta[timestamp] = value
			}
		}
	}

	alert := Alert{
		meta: allMeta,
	}

	if len(e.Body()) > 0 {
		s := string(e.Body())
		alert[Body] = splitBody(s, lineLimit)
	}

	for k, v := range e.Header() {
		if k != Body {
			alert[k] = v
			continue
		}

		if value, ok := v.(string); ok {
			alert[Body] = splitBody(value, lineLimit)
		}
	}

	return alert
}

func splitBody(s string, lineLimit int) []string {
	split := make([]string, 0)
	for i, s := range strings.Split(s, "\n") {
		if i > lineLimit {
			log.Info("body exceeds line limit %d", lineLimit)
			break
		}
		split = append(split, strings.TrimSpace(s))
	}
	return split
}

func ErrorToEvent(message string) *api.Event {
	header := make(map[string]interface{})
	var e api.Event
	var meta api.Meta
	meta = NewDefaultMeta()

	meta.Set(SystemProductTimeKey, time.Now())
	e = NewEvent(header, []byte(message))
	e.Header()[ReasonKey] = loggieError
	if len(log.AfterErrorConfig.Additions) > 0 {
		e.Header()[Addition] = log.AfterErrorConfig.Additions
	}

	if len(log.AfterErrorConfig.Fields) > 0 {
		e.Header()[Fields] = log.AfterErrorConfig.Fields
	}
	e.Fill(meta, header, e.Body())
	return &e
}

func ErrorIntoEvent(event api.Event, message string) api.Event {
	header := make(map[string]interface{})
	var meta api.Meta
	meta = NewDefaultMeta()
	meta.Set(SystemProductTimeKey, time.Now())
	header[ReasonKey] = loggieError
	if len(log.AfterErrorConfig.Additions) > 0 {
		header[Addition] = log.AfterErrorConfig.Additions
	}

	if len(log.AfterErrorConfig.Fields) > 0 {
		header[Fields] = log.AfterErrorConfig.Fields
	}
	event.Fill(meta, header, []byte(message))
	return event
}

func GenAlertsOriginData(alerts []Alert) map[string]interface{} {
	return map[string]interface{}{
		AlertOriginDataKey: alerts,
	}
}
