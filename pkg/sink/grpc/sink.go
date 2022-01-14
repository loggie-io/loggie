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

package grpc

import (
	"context"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	pb "github.com/loggie-io/loggie/pkg/sink/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"io"
	"strings"
	"time"
)

const Type = "grpc"

var (
	json = jsoniter.ConfigFastest
)

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink(info)
}

type Sink struct {
	stop        bool
	name        string
	config      *Config
	setting     map[string]interface{}
	hosts       []string
	loadBalance string
	timeout     time.Duration
	epoch       int
	logClient   pb.LogServiceClient
	conn        *grpc.ClientConn
}

func NewSink(info pipeline.Info) *Sink {
	return &Sink{
		stop:   info.Stop,
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
	return fmt.Sprintf("%s/%s: target host(%s)", api.SINK, Type, s.config.Host)
}

func (s *Sink) Init(context api.Context) {
	s.name = context.Name()
	s.setting = context.Properties()

	hosts := s.config.Host
	s.hosts = strings.Split(hosts, ",")
	s.loadBalance = s.config.LoadBalance
	s.timeout = s.config.Timeout
}

func (s *Sink) Start() {
	// register grpc name resolver
	resolver.Register(NewBuilder(s.hosts))
	// init grpc client
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:///%s", collectorScheme, collectorServiceName),
		grpc.WithInsecure(),
		grpc.WithBalancerName(s.loadBalance),
		grpc.WithInitialWindowSize(256),
	)
	if err != nil {
		log.Panic("grpc client connect error. err: %v. server hosts: %s", err, s.hosts)
	}
	s.conn = conn
	s.logClient = pb.NewLogServiceClient(conn)
	log.Info("%s start, hosts: %v, load balance: %s", s.String(), s.hosts, s.loadBalance)
}

func (s *Sink) Stop() {
	if s.conn != nil {
		_ = s.conn.Close()
	}
}

func (s *Sink) Consume(batch api.Batch) api.Result {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	stream, err := s.logClient.LogStream(ctx, grpc.WaitForReady(true))
	if err != nil {
		return result.Fail(err)
	}

	events := batch.Events()
	for _, e := range events {
		logMsg := &pb.LogMsg{
			RawLog: e.Body(),
		}
		eHeader := e.Header()

		// structured log data
		logBody, ok := eHeader["systemLogBody"]
		if ok {
			delete(eHeader, "systemLogBody")
			lb, covert := logBody.(map[string]string)
			if covert {
				lbl := len(lb)
				if lbl > 0 {
					logBodyByte := make(map[string][]byte, lbl)
					for k, v := range lb {
						logBodyByte[k] = []byte(v)
					}
					logMsg.LogBody = logBodyByte
					logMsg.IsSplit = true
				}
			}
		}

		// header & packedHeader
		grpcHeaderKey := s.config.GrpcHeaderKey
		if grpcHeaderKey != "" {
			grpcHeader, ok := eHeader[grpcHeaderKey].(map[string][]byte)
			if ok {
				logMsg.Header = grpcHeader
			} else {
				log.Error("grpc header must be map[string][]byte: %v", grpcHeader)
			}
		} else {
			packedHeader, err := json.Marshal(eHeader)
			if err != nil {
				log.Warn("Marshal event header error: %s", err)
				continue
			}
			logMsg.PackedHeader = packedHeader
		}

		err = stream.Send(logMsg)
		if err != nil && err != io.EOF {
			ls := logMsg.String()
			log.Error("%s => grpc sink send error. err: %v; raw log content: %v", s.String(), err, ls)
			return result.Fail(err)
		}
	}
	logResp, err := stream.CloseAndRecv()
	if err != nil {
		log.Error("%s => get grpc response error: %v", s.String(), err)
		return result.Fail(err)
	}
	if !logResp.Success {
		log.Error("%s => get grpc response error: %v", s.String(), logResp.ErrorMsg)
		return result.Fail(err)
	}
	return result.Success()
}
