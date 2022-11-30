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
	"fmt"
	"io"
	"net"

	jsoniter "github.com/json-iterator/go"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	pb "github.com/loggie-io/loggie/pkg/sink/grpc/pb"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

const Type = "grpc"

var (
	json = jsoniter.ConfigFastest
)

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	return &Source{
		eventPool: info.EventPool,
		config:    &Config{},
	}
}

type Source struct {
	pb.UnimplementedLogServiceServer
	name        string
	eventPool   *event.Pool
	config      *Config
	grpcServer  *grpc.Server
	bc          *batchChain
	productFunc api.ProductFunc
}

func (s *Source) Config() interface{} {
	return s.config
}

func (s *Source) Category() api.Category {
	return api.SOURCE
}

func (s *Source) Type() api.Type {
	return Type
}

func (s *Source) String() string {
	return fmt.Sprintf("%s/%s", api.SOURCE, Type)
}

func (s *Source) Init(context api.Context) error {
	s.name = context.Name()
	return nil
}

func (s *Source) Start() error {
	return nil
}

func (s *Source) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.bc != nil {
		s.bc.stop()
	}
}

func (s *Source) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", s.String())
	s.productFunc = productFunc
	s.bc = newBatchChain(s.config.MaintenanceInterval)
	go s.bc.run()
	// start grpc server
	ip := fmt.Sprintf("%s:%s", s.config.Bind, s.config.Port)
	listener, err := net.Listen(s.config.Network, ip)
	if err != nil {
		log.Panic("grpc server listen ip(%s) err: %v", ip, err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterLogServiceServer(grpcServer, s)
	go grpcServer.Serve(listener)
	s.grpcServer = grpcServer
	log.Info("grpc server start listing: %s", ip)
}

func (s *Source) Commit(events []api.Event) {
	s.bc.ack(events)
	s.eventPool.PutAll(events)
}

func (s *Source) LogStream(ls pb.LogService_LogStreamServer) error {
	b := s.bc.NewBatch(s.config.Timeout)
	for {
		logMsg, err := ls.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			sendErr := ls.SendAndClose(&pb.LogResp{
				Success:  false,
				Count:    0,
				ErrorMsg: err.Error(),
			})
			if sendErr != nil {
				log.Warn("send response fail: %s", sendErr)
			}
			return err
		}
		header := make(map[string]interface{})
		rawHeader := logMsg.GetHeader()
		if len(rawHeader) > 0 {
			for k, v := range rawHeader {
				header[k] = string(v)
			}
		}
		packedHeader := logMsg.PackedHeader
		if len(packedHeader) > 0 {
			err = json.Unmarshal(packedHeader, &header)
			if err != nil {
				log.Warn("Unmarshal packedHeader error: %s", err)
			}
		}
		e := s.eventPool.Get()
		e.Fill(e.Meta(), header, logMsg.GetRawLog())
		b.Append(e)
		s.productFunc(e)
	}
	logResp := b.Wait()
	err := ls.SendAndClose(logResp)
	if err != nil {
		log.Error("send response fail: %s", err)
	}
	return err
}
