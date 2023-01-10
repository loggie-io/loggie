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

package alertmanager

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/logalert"
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/loggie-io/loggie/pkg/util/bufferpool"
	"github.com/loggie-io/loggie/pkg/util/pattern"
)

type AlertManager struct {
	Address []string

	Client      http.Client
	temp        *template.Template
	bp          *bufferpool.BufferPool
	headers     map[string]string
	method      string
	LineLimit   int
	groupConfig logalert.GroupConfig

	lock sync.Mutex

	alertMap map[string][]event.Alert
}

type ResetTempEvent struct {
}

func NewAlertManager(config *logalert.Config) *AlertManager {
	cli := http.Client{
		Timeout: config.Timeout,
	}

	manager := &AlertManager{
		Address:   config.Addr,
		Client:    cli,
		headers:   config.Headers,
		bp:        bufferpool.NewBufferPool(1024),
		LineLimit: config.LineLimit,
		method:    http.MethodPost,
		alertMap:  make(map[string][]event.Alert),
	}

	if strings.ToUpper(config.Method) == http.MethodPut {
		manager.method = http.MethodPut
	}

	if len(config.Template) > 0 {
		t, err := template.New("alertTemplate").Parse(config.Template)
		if err != nil {
			log.Warn("fail to generate temp %s", config.Template)
		} else {
			manager.temp = t
		}
	}

	manager.groupConfig.AlertSendingThreshold = config.AlertSendingThreshold
	log.Debug("alertManager groupKey: %s", config.GroupKey)
	if len(config.GroupKey) > 0 {
		p, err := pattern.Init(config.GroupKey)
		if err != nil {
			log.Warn("fail to init group key %s: %s", config.GroupKey, err.Error())
		} else {
			manager.groupConfig.Pattern = p
		}
	}

	return manager
}

type AlertEvent struct {
	StartsAt    time.Time         `json:"StartsAt,omitempty"`
	EndsAt      time.Time         `json:"EndsAt,omitempty"`
	Labels      map[string]string `json:"Labels,omitempty"`
	Annotations map[string]string `json:"Annotations,omitempty"`
}

func (a *AlertManager) SendAlert(events []*api.Event, sendAtOnce bool) {

	var alerts []event.Alert
	for _, e := range events {
		alert := event.NewAlert(*e, a.LineLimit)
		alerts = append(alerts, alert)
	}

	if sendAtOnce {
		logalert.GroupAlertsAtOnce(alerts, a.packageAndSendAlerts, a.groupConfig)
		return
	}

	logalert.GroupAlerts(a.alertMap, alerts, a.packageAndSendAlerts, a.groupConfig)
}

func (a *AlertManager) packageAndSendAlerts(alerts []event.Alert) {
	if len(alerts) == 0 {
		return
	}

	alertCenterObj := event.GenAlertsOriginData(alerts)

	var request []byte

	a.lock.Lock()
	if a.temp != nil {
		buffer := a.bp.Get()
		defer a.bp.Put(buffer)
		err := a.temp.Execute(buffer, alertCenterObj)
		if err != nil {
			log.Warn(err.Error())
			return
		}
		request = bytes.Trim(buffer.Bytes(), "\x00")
	} else {
		out, err := json.Marshal(alertCenterObj)
		if err != nil {
			log.Warn(err.Error())
			return
		}
		request = out
	}
	a.lock.Unlock()

	log.Debug("sending alert %s", request)
	for _, address := range a.Address { // send alerts to alertManager cluster, no need to retry
		a.send(address, request)
	}
}

func (a *AlertManager) send(address string, alert []byte) {
	req, err := http.NewRequest(a.method, address, bytes.NewReader(alert))
	if err != nil {
		log.Warn("send alert error: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if len(a.headers) > 0 {
		for k, v := range a.headers {
			req.Header.Set(k, v)
		}
	}
	resp, err := a.Client.Do(req)
	if err != nil {
		log.Warn("send alert error: %v", err)
		return
	}
	defer resp.Body.Close()

	if !util.Is2xxSuccess(resp.StatusCode) {
		r, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Warn("read response body error: %v", err)
		}
		log.Warn("send alert failed, response statusCode: %d, body: %s", resp.StatusCode, r)
	}
}

func (a *AlertManager) UpdateTemp(temp string) {
	a.lock.Lock()
	defer a.lock.Unlock()

	t, err := template.New("alertTemplate").Parse(temp)
	if err != nil {
		log.Warn("fail to generate temp %s", temp)
	}
	a.temp = t
}
