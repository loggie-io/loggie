package webhook

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/loggie-io/loggie/pkg/source/file"
	"io/ioutil"
	"net/http"
	"text/template"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/global"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
)

const Type = "webhook"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Alert struct {
	Event  map[string]interface{} `json:"Event,omitempty"`
	System map[string]interface{} `json:"System,omitempty"`
	State  *file.State            `json:"State,omitempty"`
}

type Sink struct {
	name   string
	config *Config
	codec  codec.Codec
	temp   *template.Template
	bp     *BufferPool
	client *http.Client
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

	return nil
}

func (s *Sink) Stop() {
}

func (s *Sink) Consume(batch api.Batch) api.Result {
	events := batch.Events()
	l := len(events)
	if l == 0 {
		return nil
	}

	var alerts []Alert
	for _, e := range events {
		eventData := map[string]interface{}{
			"Host":    global.NodeName,
			"Source":  e.Meta().Source(),
			"Message": string(e.Body()),
		}

		systemData := map[string]interface{}{
			"Time": time.Now(),
		}

		var state *file.State
		if stateValue, ok := e.Meta().GetAll()[file.SystemStateKey].(*file.State); ok {
			state = stateValue
		}

		alert := Alert{
			Event:  eventData,
			System: systemData,
			State:  state,
		}

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
		log.Warn("post alert to AlertManager error: %v", err)
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
		log.Warn("post alert to AlertCenter error: %v", err)
		return result.Fail(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		r, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Warn("read response body error: %v", err)
			return result.Fail(err)
		}
		log.Warn("post alert to AlertCenter failed, response statusCode: %d, body: %v", resp.StatusCode, string(r))
		return result.Fail(
			errors.New(
				fmt.Sprintf("post alert to AlertCenter failed, response statusCode: %d, body: %v", resp.StatusCode, string(r))))
	}

	return result.NewResult(api.SUCCESS)

}
