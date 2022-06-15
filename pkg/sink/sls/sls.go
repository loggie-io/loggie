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

package sls

import (
	"fmt"
	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/gogo/protobuf/proto"
	"github.com/loggie-io/loggie/pkg/core/api"
	eventer "github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/global"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/pkg/errors"
	"time"
)

const Type = "sls"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Sink struct {
	config *Config

	client sls.ClientInterface
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

func (s *Sink) String() string {
	return fmt.Sprintf("%s/%s", api.SINK, Type)
}

func (s *Sink) Init(context api.Context) error {
	return nil
}

func (s *Sink) Start() error {
	log.Info("starting %s", s.String())
	conf := s.config
	s.client = sls.CreateNormalInterface(conf.Endpoint, conf.AccessKeyId, conf.AccessKeySecret, "")

	// Check if project exist
	exist, err := s.client.CheckProjectExist(conf.Project)
	if err != nil {
		return errors.WithMessagef(err, "Check sls project %s failed", conf.Project)
	}
	if !exist {
		return errors.Errorf("Project %s is not exist", conf.Project)
	}

	// Check if LogStore exist
	exist, err = s.client.CheckLogstoreExist(conf.Project, conf.LogStore)
	if err != nil {
		return errors.WithMessagef(err, "Check logstore %s failed", conf.LogStore)
	}
	if !exist {
		return errors.Errorf("Logstore %s is not exist", conf.LogStore)
	}

	s.client.SetUserAgent(sls.DefaultLogUserAgent + " loggie/" + global.GetVersion())

	return nil
}

func (s *Sink) Stop() {
	if s.client != nil {
		s.client.Close()
	}
}

func (s *Sink) Consume(batch api.Batch) api.Result {
	// convert events to sls logs
	logs := genSlsLogs(batch)

	logGroup := &sls.LogGroup{
		Topic:  proto.String(s.config.Topic),
		Source: proto.String(global.NodeName),
		Logs:   logs,
	}

	if err := s.client.PutLogs(s.config.Project, s.config.LogStore, logGroup); err != nil {
		return result.Fail(err)
	}

	return result.NewResult(api.SUCCESS)
}

const token = "."

func genSlsLogs(batch api.Batch) []*sls.Log {
	var logs []*sls.Log

	for _, e := range batch.Events() {
		logData := &sls.Log{}
		if timestamp, exist := e.Meta().Get(eventer.SystemProductTimeKey); exist {
			if t, ok := timestamp.(time.Time); ok {
				logData.Time = proto.Uint32(uint32(t.Unix()))
			}
		}

		obj := runtime.NewObject(e.Header())
		flatHeader, err := obj.FlatKeyValue(token)
		if err != nil {
			log.Error("flatten key/value pair in events error: %v", err)
			log.Debug("flatten failed events: %s", e.String())
			continue
		}

		var contents []*sls.LogContent
		for k, v := range flatHeader {
			sv, ok := v.(string)
			if !ok {
				continue // TODO(ethfoo) convert to string instead of drop fields
			}

			logContent := &sls.LogContent{
				Key:   proto.String(k),
				Value: proto.String(sv),
			}
			contents = append(contents, logContent)
		}

		if len(e.Body()) > 0 {
			contents = append(contents, &sls.LogContent{
				Key:   proto.String(eventer.Body),
				Value: proto.String(string(e.Body())),
			})
		}

		logData.Contents = contents

		logs = append(logs, logData)
	}

	return logs
}
