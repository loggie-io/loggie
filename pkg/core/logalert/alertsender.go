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

package logalert

import (
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/loggie-io/loggie/pkg/util/runtime"
)

const (
	DefaultAlertKey = event.DefaultAlertKey
)

type GroupConfig struct {
	Pattern               *pattern.Pattern
	AlertSendingThreshold int
}

func GroupAlerts(alertMap event.AlertMap, alerts []event.Alert, sendFunc event.PackageAndSendAlerts, p GroupConfig) {
	checkAlertsLists := func(key string) {
		list := alertMap[key]
		if len(list) >= p.AlertSendingThreshold {
			sendFunc(list)
			alertMap[key] = nil
		}
	}

	if p.Pattern == nil {
		alertMap[DefaultAlertKey] = append(alertMap[DefaultAlertKey], alerts...)
		checkAlertsLists(DefaultAlertKey)
	} else {
		keyMap := make(map[string]struct{})
		for _, alert := range alerts {
			obj := runtime.NewObject(alert)
			render, err := p.Pattern.WithObject(obj).Render()
			if err != nil {
				log.Warn("fail to render group key. Put alert in default group")
				alertMap[DefaultAlertKey] = append(alertMap[DefaultAlertKey], alert)
				keyMap[DefaultAlertKey] = struct{}{}
				continue
			}

			log.Debug("alert group key %s", render)
			alertMap[render] = append(alertMap[render], alert)
			keyMap[render] = struct{}{}
		}

		for key := range keyMap {
			checkAlertsLists(key)
		}
	}

}

func GroupAlertsAtOnce(alerts []event.Alert, sendFunc event.PackageAndSendAlerts, p GroupConfig) {
	if p.Pattern == nil {
		sendFunc(alerts)
	} else {
		tempMap := make(event.AlertMap)
		for _, alert := range alerts {
			obj := runtime.NewObject(alert)
			render, err := p.Pattern.WithObject(obj).Render()
			if err != nil {
				log.Warn("fail to render group key. Put alert in default group")
				tempMap[DefaultAlertKey] = append(tempMap[DefaultAlertKey], alert)
				continue
			}

			log.Debug("alert group key %s", render)
			tempMap[render] = append(tempMap[render], alert)
		}

		for _, list := range tempMap {
			sendFunc(list)
		}
	}

}
