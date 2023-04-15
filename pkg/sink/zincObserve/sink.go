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

package zincObserve

import (
	"crypto/tls"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"net/http"
	"strings"
)

const Type = "zincObserve"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Sink struct {
	name    string
	config  *Config
	codec   codec.Codec
	client  *http.Client
	pushUrl string
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
	s.client = &http.Client{}
	if s.config.SkipSSLVerify {
		s.client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}
	s.pushUrl = fmt.Sprintf("%s/api/%s/%s/_json", s.config.Host, s.config.Org, s.config.Index)
	fmt.Println("s.pushUrl>>>>", s.pushUrl)
	return nil
}

func (s *Sink) Start() error {
	log.Info("%s start", s.String())
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
	dataList := make([]string, 0)
	for k, e := range events {
		// json encode
		out, err := s.codec.Encode(e)
		if err != nil {
			log.Warn("codec event error: %+v", err)
			continue
		}
		dataList = append(dataList, string(out)+",\n")
		if k+1 == len(events) {
			dataList = append(dataList, string(out))
		}
	}
	t := fmt.Sprintf("%v", dataList)
	req, err := http.NewRequest("POST", s.pushUrl, strings.NewReader(t))
	if err != nil {
		return result.Fail(err)
	}
	req.SetBasicAuth(s.config.Username, s.config.Password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return result.Fail(err)
	}
	defer resp.Body.Close()
	return result.NewResult(api.SUCCESS)
}
