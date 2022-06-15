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

package zinc

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
)

const Type = "zinc"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Sink struct {
	name   string
	config *Config
	codec  codec.Codec
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

	if !s.config.PrintEvents {
		return result.NewResult(api.SUCCESS)
	}
	dataList := make([]string, 0)
	for _, e := range events {
		// json encode
		out, err := s.codec.Encode(e)
		if err != nil {
			log.Warn("codec event error: %+v", err)
			continue
		}
		log.Info("event: %s", string(out))
		dataList = append(dataList, fmt.Sprintf("{ \"index\" : { \"_index\" : \"%s\" } }", s.config.Index))
		dataList = append(dataList, string(out))
	}
	ndjson := strings.Join(dataList, "\n")
	log.Info(ndjson)
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/_bulk", s.config.Host), strings.NewReader(ndjson))
	if err != nil {
		return result.Fail(err)
	}
	req.SetBasicAuth(s.config.Username, s.config.Password)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return result.Fail(err)
	}
	defer resp.Body.Close()
	log.Info("StatusCode: %d", resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return result.Fail(err)
	}
	fmt.Println(string(body))
	return result.NewResult(api.SUCCESS)
}
