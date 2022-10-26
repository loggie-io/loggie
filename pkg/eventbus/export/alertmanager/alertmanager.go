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
	"sync"
	"text/template"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/sink/webhook"
)

type AlertManager struct {
	Address []string

	Client  http.Client
	temp    *template.Template
	bp      *BufferPool
	headers map[string]string

	lock sync.Mutex
}

type ResetTempEvent struct {
}

func NewAlertManager(addr []string, timeout int, temp *string, headers map[string]string) *AlertManager {
	cli := http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}
	manager := &AlertManager{
		Address: addr,
		Client:  cli,
		headers: headers,
		bp:      newBufferPool(1024),
	}

	if temp != nil {
		t, err := template.New("test").Parse(*temp)
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

	var alerts []*webhook.Alert
	for _, e := range events {

		if e.Data == nil {
			continue
		}

		data, ok := e.Data.(*api.Event)
		if !ok {
			log.Info("fail to convert data to event")
			return
		}

		alert := webhook.NewAlert(*data)

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

	for _, address := range a.Address { // send alerts to alertManager cluster, no need to retry
		a.send(address, request)
	}
}

func (a *AlertManager) send(address string, alert []byte) {
	req, err := http.NewRequest("POST", address, bytes.NewReader(alert))
	if err != nil {
		log.Warn("post alert error: %v", err)
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
		log.Warn("post alert error: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		r, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Warn("read response body error: %v", err)
		}
		log.Warn("post alert failed, response statusCode: %d, body: %v", resp.StatusCode, string(r))
	}
	log.Debug("send alerts %s success", string(alert))
}

func (a *AlertManager) UpdateTemp(temp string) {
	t, err := template.New("test").Parse(temp)
	if err != nil {
		log.Error("fail to generate temp %s", temp)
	}
	a.temp = t
}
