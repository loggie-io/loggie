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

package file

import (
	"fmt"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/global"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/source/codec"
	"github.com/loggie-io/loggie/pkg/util/persistence"
)

const Type = "file"

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
	globalLineEnd.Init()
}

func makeSource(info pipeline.Info) api.Component {
	return &Source{
		pipelineName: info.PipelineName,
		epoch:        info.Epoch,
		rc:           info.R,
		eventPool:    info.EventPool,
		sinkCount:    info.SinkCount,
		config:       &Config{},
	}
}

type Source struct {
	pipelineName       string
	epoch              *pipeline.Epoch
	rc                 *pipeline.RegisterCenter
	eventPool          *event.Pool
	config             *Config
	rawSourceConfig    *source.Config
	sinkCount          int
	name               string
	out                chan api.Event
	productFunc        api.ProductFunc
	r                  *Reader
	ackEnable          bool
	ackChainHandler    *AckChainHandler
	watcher            *Watcher
	watchTask          *WatchTask
	ackTask            *AckTask
	dbHandler          *persistence.DbHandler
	isolation          Isolation
	multilineProcessor *MultiProcessor
	mTask              *MultiTask
	codec              codec.Codec
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
	return fmt.Sprintf("%s/%s/%s", s.Category(), s.Type(), s.name)
}

func (s *Source) SetCodec(c codec.Codec) {
	s.codec = c
}

func (s *Source) SetSourceConfig(config *source.Config) {
	s.rawSourceConfig = config
}

func (s *Source) Init(context api.Context) error {
	s.name = context.Name()
	s.out = make(chan api.Event, s.sinkCount)

	s.ackEnable = s.config.AckConfig.Enable
	// init default multi agg timeout
	multiTimeout := s.config.ReaderConfig.MultiConfig.Timeout
	inactiveTimeout := s.config.ReaderConfig.InactiveTimeout
	if multiTimeout == 0 || multiTimeout <= inactiveTimeout {
		s.config.ReaderConfig.MultiConfig.Timeout = 2 * inactiveTimeout
	}

	// init reader chan size
	s.config.ReaderConfig.readChanSize = s.config.WatchConfig.MaxOpenFds

	// check
	cleanInactiveTimeout := persistence.GetConfig().CleanInactiveTimeout
	if inactiveTimeout > cleanInactiveTimeout {
		cleanInactiveTimeout = 2 * inactiveTimeout
		if cleanInactiveTimeout < time.Hour {
			cleanInactiveTimeout = time.Hour
		}
		log.Info("db CleanInactiveTimeout cannot be small than read InactiveTimeout,change to %dh", cleanInactiveTimeout/time.Hour)
	}

	s.isolation = Isolation{
		PipelineName: s.pipelineName,
		SourceName:   s.name,
		Level:        IsolationLevel(s.config.Isolation),
	}
	globalLineEnd.AddLineEnd(s.pipelineName, s.name, &s.config.ReaderConfig.LineDelimiter)
	return nil
}

func (s *Source) Start() error {
	log.Info("start source: %s", s.String())
	if s.config.ReaderConfig.MultiConfig.Active {
		s.multilineProcessor = GetOrCreateShareMultilineProcessor()
	}
	// register queue listener for ack
	if s.ackEnable {
		s.dbHandler = persistence.GetOrCreateShareDbHandler()
		s.ackChainHandler = GetOrCreateShareAckChainHandler(s.sinkCount, s.config.AckConfig)
		s.rc.RegisterListener(&AckListener{
			sourceName:      s.name,
			ackChainHandler: s.ackChainHandler,
		})
	}

	s.watcher = GetOrCreateShareWatcher(s.config.WatchConfig)
	s.r = GetOrCreateReader(s.isolation, s.config.ReaderConfig, s.watcher)

	s.HandleHttp()
	return nil
}

func (s *Source) Stop() {
	log.Info("start stop source: %s", s.String())
	// Stop ack
	if s.ackEnable {
		// stop append&ack source event
		s.ackChainHandler.StopTask(s.ackTask)
		log.Info("[%s] all ack jobs of source exit", s.String())
	}
	// Stop watch task
	if s.watchTask != nil {
		s.watcher.StopWatchTask(s.watchTask)
	}
	log.Info("[%s] watch task stop", s.String())
	// Stop reader
	StopReader(s.isolation)
	log.Info("[%s] reader stop", s.String())
	// Stop multilineProcessor
	if s.config.ReaderConfig.MultiConfig.Active {
		s.multilineProcessor.StopTask(s.mTask)
	}
	globalLineEnd.RemoveLineEnd(s.pipelineName, s.name)
	log.Info("source has stopped: %s", s.String())
}

func (s *Source) Product() api.Event {
	return <-s.out
}

func (s *Source) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", s.String())
	s.productFunc = productFunc
	s.productFunc = jobFieldsProductFunc(s.productFunc, s.rawSourceConfig)
	if s.config.CollectConfig.AddonMeta {
		s.productFunc = addonMetaProductFunc(s.productFunc)
	}
	if s.config.ReaderConfig.MultiConfig.Active {
		s.mTask = NewMultiTask(s.epoch, s.name, s.config.ReaderConfig.MultiConfig, s.eventPool, s.productFunc)
		s.multilineProcessor.StartTask(s.mTask)
		s.productFunc = s.multilineProcessor.Process
	}
	if s.codec != nil {
		s.productFunc = codec.ProductFunc(s.productFunc, s.codec)
	}
	if s.config.CollectConfig.Charset != "utf-8" {
		s.productFunc = NewCharset(s.config.CollectConfig.Charset, s.productFunc).Hook
	}
	if s.ackEnable {
		s.ackTask = NewAckTask(s.epoch, s.pipelineName, s.name, func(state *persistence.State) {
			s.dbHandler.State <- state
		})
		s.ackChainHandler.StartTask(s.ackTask)
		log.Info("%s ack start", s.String())
	}
	s.watchTask = NewWatchTask(s.epoch, s.pipelineName, s.name, s.config.CollectConfig, s.eventPool, s.productFunc, s.r.jobChan, s.rawSourceConfig.Fields)
	// start watch source paths
	s.watcher.StartWatchTask(s.watchTask)
}

func (s *Source) Commit(events []api.Event) {
	// ack events
	if s.ackEnable {
		ss := make([]*persistence.State, 0, len(events))
		for _, e := range events {
			ss = append(ss, getState(e))
		}
		s.ackChainHandler.ackChan <- ss
	}
	// release events
	s.eventPool.PutAll(events)
}

func jobFieldsProductFunc(productFunc api.ProductFunc, srcCfg *source.Config) api.ProductFunc {
	return func(event api.Event) api.Result {
		s, _ := event.Meta().Get(SystemStateKey)
		state := s.(*persistence.State)

		if state.JobFields != nil {
			pipeline.AddSourceFields(event.Header(), state.JobFields, srcCfg.FieldsUnderRoot, srcCfg.FieldsUnderKey)
		}

		productFunc(event)
		return result.Success()
	}
}

func addonMetaProductFunc(productFunc api.ProductFunc) api.ProductFunc {
	return func(event api.Event) api.Result {
		s, _ := event.Meta().Get(SystemStateKey)
		state := s.(*persistence.State)
		addonMeta := make(map[string]interface{})
		addonMeta["pipeline"] = state.PipelineName
		addonMeta["source"] = state.SourceName
		addonMeta["filename"] = state.Filename
		addonMeta["timestamp"] = state.CollectTime.Local().Format(tsLayout)
		addonMeta["offset"] = state.Offset
		addonMeta["bytes"] = state.ContentBytes
		addonMeta["hostname"] = global.NodeName

		event.Header()["state"] = addonMeta
		productFunc(event)
		return result.Success()
	}
}
