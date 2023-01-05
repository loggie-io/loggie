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
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/sink/webhook"
	"github.com/loggie-io/loggie/pkg/util/bufferpool"
	"github.com/loggie-io/loggie/pkg/sink/alertwebhook"
)

type AlertManager struct {
	Address []string

	Client    http.Client
	temp      *template.Template
	bp        *bufferpool.BufferPool
	headers   map[string]string
	method    string
	LineLimit int

	lock sync.Mutex
}

type ResetTempEvent struct {
}

func NewAlertManager(addr []string, timeout, lineLimit int, temp *string, headers map[string]string, method string) *AlertManager {
	cli := http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}
	manager := &AlertManager{
		Address:   addr,
		Client:    cli,
		headers:   headers,
		bp:        bufferpool.NewBufferPool(1024),
		LineLimit: lineLimit,
		method:    http.MethodPost,
	}

	if strings.ToUpper(method) == http.MethodPut {
		manager.method = http.MethodPost
	}

	if temp != nil {
		t, err := template.New("alertTemplate").Parse(*temp)
		if err != nil {
			log.Error("fail to generate temp %s", *temp)
		}
		manager.temp = t
	}
	return manager
}

type AlertEvent struct {
	StartsAt    time.Time         `json:"StartsAt,omitempty"`
	EndsAt      time.Time         `json:"EndsAt,omitempty"`
	Labels      map[string]string `json:"Labels,omitempty"`
	Annotations map[string]string `json:"Annotations,omitempty"`
}

func (a *AlertManager) SendAlert(events []*eventbus.Event) {

	var alerts []*alertwebhook.Alert
	for _, e := range events {

		if e.Data == nil {
			continue
		}

		data, ok := e.Data.(*api.Event)
		if !ok {
			log.Info("fail to convert data to event")
			return
		}

		alert := alertwebhook.NewAlert(*data, a.LineLimit)

		alerts = append(alerts, &alert)
	}

	alertCenterObj := map[string]interface{}{
		"Alerts": alerts,
	}

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

	if !alertwebhook.Is2xxSuccess(resp.StatusCode) {
		r, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Warn("read response body error: %v", err)
		}
		log.Warn("send alert failed, response statusCode: %d, body: %s", resp.StatusCode, r)
	}
}

func (a *AlertManager) UpdateTemp(temp string) {
	t, err := template.New("alertTemplate").Parse(temp)
	if err != nil {
		log.Error("fail to generate temp %s", temp)
	}
	a.temp = t
}
