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

package webhook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"text/template"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
)

const Type = pipeline.WebhookSinkType

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Alert map[string]interface{}

func NewAlert(e api.Event, lineLimit int) Alert {
	systemData := map[string]interface{}{}

	allMeta := e.Meta().GetAll()

	if value, ok := allMeta[event.SystemSourceKey]; ok {
		systemData["sourceName"] = value
	}

	if value, ok := allMeta[event.SystemPipelineKey]; ok {
		systemData["pipelineName"] = value
	}

	if value, ok := allMeta[event.SystemProductTimeKey]; ok {
		t, valueToTime := value.(time.Time)
		if !valueToTime {
			systemData["timestamp"] = value
		} else {
			textTime, err := t.MarshalText()
			if err == nil {
				systemData["timestamp"] = string(textTime)
			} else {
				systemData["timestamp"] = value
			}
		}
	}

	alert := Alert{
		"_meta": systemData,
	}

	if len(e.Body()) > 0 {
		s := string(e.Body())
		split := make([]string, 0)
		for i, s := range strings.Split(s, "\n") {
			if i > lineLimit {
				log.Info("body exceeds line limit %d", lineLimit)
				break
			}
			split = append(split, strings.TrimSpace(s))
		}
		alert["body"] = split
	}

	for k, v := range e.Header() {
		if k == "body" {
			if value, ok := v.(string); ok {
				split := make([]string, 0)
				for i, s := range strings.Split(value, "\n") {
					if i > lineLimit {
						log.Info("body exceeds line limit %d", lineLimit)
						break
					}
					split = append(split, strings.TrimSpace(s))
				}
				alert["body"] = split
				continue
			}
		}
		alert[k] = v
	}

	return alert
}

type Sink struct {
	name      string
	config    *Config
	codec     codec.Codec
	temp      *template.Template
	bp        *BufferPool
	client    *http.Client
	subscribe *eventbus.Subscribe
	listener  *Listener
}

func NewSink() *Sink {
	return &Sink{
		config: &Config{},
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
		done: make(chan struct{}),
		sink: s,
	}
	s.subscribe = eventbus.RegistryTemporary(name, func() eventbus.Listener {
		return s.listener
	}, eventbus.WithTopic(eventbus.WebhookTopic))

	s.name = context.Name()
	s.bp = newBufferPool(1024)
	s.client = &http.Client{
		Timeout: time.Duration(s.config.Timeout) * time.Second,
	}
	return nil
}

func (s *Sink) Start() error {
	log.Info("%s start", s.String())
	t := s.config.Template
	if t != "" {
		temp, err := template.New("test").Parse(t)
		if err != nil {
			log.Error("fail to generate temp %s", t)
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
		return nil
	}

	var alerts []Alert
	for _, e := range events {

		alert := NewAlert(e, s.config.LineLimit)

		alerts = append(alerts, alert)
	}

	alertCenterObj := map[string]interface{}{
		"Alerts": alerts,
	}

	var request []byte

	if s.temp != nil {
		buffer := s.bp.Get()
		defer s.bp.Put(buffer)
		err := s.temp.Execute(buffer, alertCenterObj)
		if err != nil {
			log.Warn(err.Error())
			return result.Fail(err)
		}
		request = bytes.Trim(buffer.Bytes(), "\x00")
	} else {
		out, err := json.Marshal(alertCenterObj)
		if err != nil {
			log.Warn(err.Error())
			return result.Fail(err)
		}
		request = out
	}

	req, err := http.NewRequest("POST", s.config.Addr, bytes.NewReader(request))
	if err != nil {
		log.Warn("post alert error: %v", err)
		return result.Fail(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if len(s.config.Headers) > 0 {
		for k, v := range s.config.Headers {
			req.Header.Set(k, v)
		}
	}
	resp, err := s.client.Do(req)
	if err != nil {
		log.Warn("post alert error: %v", err)
		return result.Fail(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		r, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Warn("read response body error: %v", err)
			return result.Fail(err)
		}
		log.Warn("post alert failed, response statusCode: %d, body: %v", resp.StatusCode, string(r))
		return result.Fail(fmt.Errorf("post alert to AlertCenter failed, response statusCode: %d, body: %v", resp.StatusCode, string(r)))
	}

	return result.NewResult(api.SUCCESS)

}
