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

package alertwebhook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"text/template"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/logalert"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/loggie-io/loggie/pkg/util/pattern"
)

const (
	Type = "alertWebhook"
)

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink(info.PipelineName)
}

type Sink struct {
	pipelineName string
	name         string
	config       *Config
	codec        codec.Codec
	temp         *template.Template
	bp           *BufferPool
	client       *http.Client
	method       string
	subscribe    *eventbus.Subscribe
	listener     *Listener
	groupConfig  logalert.GroupConfig
	alertMap     event.AlertMap

	lock sync.Mutex
}

func NewSink(pipelineName string) *Sink {
	return &Sink{
		config:       &Config{},
		pipelineName: pipelineName,
	}
}

func (s *Sink) Category() api.Category {
	return api.SINK
}

func (s *Sink) Type() api.Type {
	return Type
}

func (s *Sink) Config() interface{} {
	return s.config
}

func (s *Sink) SetCodec(c codec.Codec) {
	s.codec = c
}

func (s *Sink) String() string {
	return fmt.Sprintf("%s/%s", api.SINK, Type)
}

func (s *Sink) Init(context api.Context) error {
	s.listener = &Listener{
		name:                  fmt.Sprintf("%s/%s", s.pipelineName, name),
		done:                  make(chan struct{}),
		sink:                  s,
		SendNoDataAlertAtOnce: s.config.SendNoDataAlertAtOnce,
		SendLoggieError:       s.config.SendLoggieError,
		SendLoggieErrorAtOnce: s.config.SendLoggieErrorAtOnce,
	}

	s.name = context.Name()
	s.bp = newBufferPool(1024)
	s.client = &http.Client{
		Timeout: s.config.Timeout,
	}

	if strings.ToUpper(s.config.Method) == http.MethodPut {
		s.method = http.MethodPut
	} else {
		s.method = http.MethodPost
	}

	s.groupConfig.AlertSendingThreshold = s.config.AlertSendingThreshold
	log.Debug("alertWebhook groupKey: %s", s.config.GroupKey)
	if len(s.config.GroupKey) > 0 {
		p, err := pattern.Init(s.config.GroupKey)
		if err != nil {
			return err
		}

		s.groupConfig.Pattern = p
	}

	s.alertMap = make(event.AlertMap)

	topics := []string{eventbus.NoDataTopic, eventbus.ErrorTopic}
	s.subscribe = eventbus.RegistryTemporary(s.listener.name, func() eventbus.Listener {
		return s.listener
	}, eventbus.WithTopics(topics))

	return nil
}

func (s *Sink) Start() error {
	log.Info("%s start", s.String())
	t := s.config.Template
	if t != "" {
		temp, err := template.New("alertTemplate").Parse(t)
		if err != nil {
			log.Warn("fail to generate temp %s", t)
			return err
		}
		s.temp = temp
	}

	_ = s.listener.Start()

	return nil
}

func (s *Sink) Stop() {
	eventbus.UnRegistrySubscribeTemporary(s.subscribe)
	s.listener.Stop()
}

func (s *Sink) Consume(batch api.Batch) api.Result {
	events := batch.Events()
	l := len(events)
	if l == 0 {
		return result.Success()
	}

	s.sendAlerts(events, s.config.SendLogAlertAtOnce)

	return result.Success()
}

func (s *Sink) sendAlerts(events []api.Event, sendAtOnce bool) {
	var alerts []event.Alert
	for _, e := range events {
		alert := event.NewAlert(e, s.config.LineLimit)
		alerts = append(alerts, alert)
	}

	if sendAtOnce {
		logalert.GroupAlertsAtOnce(alerts, s.packageAndSendAlerts, s.groupConfig)
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	logalert.GroupAlerts(s.alertMap, alerts, s.packageAndSendAlerts, s.groupConfig)
}

func (s *Sink) packageAndSendAlerts(alerts []event.Alert) {
	if len(alerts) == 0 {
		return
	}

	alertCenterObj := event.GenAlertsOriginData(alerts)

	var request []byte

	if s.temp != nil {
		buffer := s.bp.Get()
		defer s.bp.Put(buffer)
		err := s.temp.Execute(buffer, alertCenterObj)
		if err != nil {
			log.Warn(err.Error())
			return
		}
		// remove blank
		request = bytes.Trim(buffer.Bytes(), "\x00")
	} else {
		out, err := json.Marshal(alertCenterObj)
		if err != nil {
			log.Warn(err.Error())
			return
		}
		request = out
	}

	log.Debug("pipeline %s sending data %s", s.pipelineName, request)
	s.sendData(request)
}

func (s *Sink) sendData(request []byte) {
	if len(s.config.Addr) == 0 {
		log.Warn("no addr, ignore...")
		return
	}

	req, err := http.NewRequest(s.method, s.config.Addr, bytes.NewReader(request))
	if err != nil {
		log.Warn("send alert error: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if len(s.config.Headers) > 0 {
		for k, v := range s.config.Headers {
			req.Header.Set(k, v)
		}
	}
	resp, err := s.client.Do(req)
	if err != nil {
		log.Warn("send alert error: %v", err)
		return
	}
	defer resp.Body.Close()

	if !util.Is2xxSuccess(resp.StatusCode) {
		r, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Warn("read response body error: %v", err)
			return
		}
		log.Warn("sending alert failed, response statusCode: %d, body: %s", resp.StatusCode, r)
		return
	}

	return
}
