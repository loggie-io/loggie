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
	"github.com/loggie-io/loggie/pkg/core/log"
	"net/http"
	"time"
)

type AlertManager struct {
	Address []string

	Client http.Client
}

func NewAlertManager(addr []string) *AlertManager {
	cli := http.Client{}
	return &AlertManager{
		Address: addr,
		Client:  cli,
	}
}

type AlertEvent struct {
	StartsAt    time.Time         `json:"StartsAt,omitempty"`
	EndsAt      time.Time         `json:"EndsAt,omitempty"`
	Labels      map[string]string `json:"Labels,omitempty"`
	Annotations map[string]string `json:"Annotations,omitempty"`
}

func (a *AlertManager) SendAlert(alert []*AlertEvent) {

	out, err := json.Marshal(alert)
	if err != nil {
		log.Warn("marshal alert events error: %v", err)
		return
	}

	for _, address := range a.Address { // send alerts to alertManager cluster, no need to retry
		a.send(address, out)
	}
}

func (a *AlertManager) send(address string, alert []byte) {
	resp, err := a.Client.Post(address, "application/json", bytes.NewReader(alert))
	if err != nil {
		log.Warn("post alert to AlertManager error: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		r, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Warn("read response body error: %v", err)
		}
		log.Warn("post alert to AlertManager failed, response statusCode: %d, body: %v", resp.StatusCode, string(r))
	}
	log.Debug("send alerts %s to AlertManager success", string(alert))
}
