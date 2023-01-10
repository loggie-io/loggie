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
	Addition        = "_additions"
	Fields          = "fields"
	ReasonKey       = "reason"

	AlertOriginDataKey = "Alerts"

	loggieError = "LoggieError"
)

type Alert map[string]interface{}

type AlertMap map[string][]Alert

type PackageAndSendAlerts func(alerts []Alert)

func NewAlert(e api.Event, lineLimit int) Alert {
	systemData := map[string]interface{}{}

	allMeta := e.Meta().GetAll()

	if value, ok := allMeta[SystemSourceKey]; ok {
		systemData[sourceName] = value
	}

	if value, ok := allMeta[SystemPipelineKey]; ok {
		systemData[pipelineName] = value
	}

	if value, ok := allMeta[SystemProductTimeKey]; ok {
		t, valueToTime := value.(time.Time)
		if !valueToTime {
			systemData[timestamp] = value
		} else {
			textTime, err := t.MarshalText()
			if err == nil {
				systemData[timestamp] = string(textTime)
			} else {
				systemData[timestamp] = value
			}
		}
	}

	alert := Alert{
		meta: systemData,
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
