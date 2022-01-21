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
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/core/source/abstract"
	"loggie.io/loggie/pkg/pipeline"
	"time"
)

const Type = "file"

func init() {
	abstract.SourceRegister(Type, makeSource)
}

func makeSource(info pipeline.Info) abstract.SourceConvert {
	s := &Source{
		Source: abstract.ExtendsAbstractSource(info, Type),
		config: &Config{},
	}
	return s
}

type Source struct {
	*abstract.Source
	config             *Config
	productFunc        api.ProductFunc
	r                  *Reader
	ackEnable          bool
	ackChainHandler    *AckChainHandler
	watcher            *Watcher
	watchTask          *WatchTask
	ackTask            *AckTask
	dbHandler          *dbHandler
	isolation          Isolation
	multilineProcessor *MultiProcessor
	mTask              *MultiTask
}

// ------------------------------------------------------------------------
//  override methods
// ------------------------------------------------------------------------

func (s *Source) DoStart(context api.Context) {
	s.ackEnable = s.config.AckConfig.Enable
	// init default multi agg timeout
	mutiTimeout := s.config.ReaderConfig.MultiConfig.Timeout
	inactiveTimeout := s.config.ReaderConfig.InactiveTimeout
	if mutiTimeout == 0 || mutiTimeout <= inactiveTimeout {
		s.config.ReaderConfig.MultiConfig.Timeout = 2 * inactiveTimeout
	}

	// check
	cleanInactiveTimeout := s.config.DbConfig.CleanInactiveTimeout
	if inactiveTimeout > cleanInactiveTimeout {
		cleanInactiveTimeout = 2 * inactiveTimeout
		if cleanInactiveTimeout < time.Hour {
			cleanInactiveTimeout = time.Hour
		}
		log.Info("db CleanInactiveTimeout cannot be small than read InactiveTimeout,change to %dh", cleanInactiveTimeout/time.Hour)
	}

	s.isolation = Isolation{
		PipelineName: s.PipelineName(),
		SourceName:   s.Name(),
		Level:        IsolationLevel(s.config.Isolation),
	}

	if s.config.ReaderConfig.MultiConfig.Active {
		s.multilineProcessor = GetOrCreateShareMultilineProcessor()
	}
	// register queue listener for ack
	if s.ackEnable {
		s.dbHandler = GetOrCreateShareDbHandler(s.config.DbConfig)
		s.ackChainHandler = GetOrCreateShareAckChainHandler(s.PipelineInfo().SinkCount, s.config.AckConfig)
		s.PipelineInfo().R.RegisterListener(&AckListener{
			sourceName:      s.Name(),
			ackChainHandler: s.ackChainHandler,
		})
	}

	s.watcher = GetOrCreateShareWatcher(s.config.WatchConfig, s.config.DbConfig)
	s.r = GetOrCreateReader(s.isolation, s.config.ReaderConfig, s.watcher)

	s.HandleHttp()
}

func (s *Source) DoStop() {
	// Stop ack
	if s.ackEnable {
		// stop append&ack source event
		s.ackChainHandler.StopTask(s.ackTask)
		log.Info("[%s] all ack jobs of source exit", s.String())
	}
	// Stop watch task
	s.watcher.StopWatchTask(s.watchTask)
	log.Info("[%s] watch task stop", s.String())
	// Stop reader
	StopReader(s.isolation)
	log.Info("[%s] reader stop", s.String())
	// Stop multilineProcessor
	if s.config.ReaderConfig.MultiConfig.Active {
		s.multilineProcessor.StopTask(s.mTask)
	}
}

func (s *Source) DoCommit(events []api.Event) {
	// ack events
	if s.ackEnable {
		ss := make([]*State, 0, len(events))
		for _, e := range events {
			ss = append(ss, getState(e))
		}
		s.ackChainHandler.ackChan <- ss
	}
}

func (s *Source) Config() interface{} {
	return s.config
}

func (s *Source) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", s.String())
	s.productFunc = productFunc
	if s.config.ReaderConfig.MultiConfig.Active {
		s.mTask = NewMultiTask(s.Epoch(), s.Name(), s.config.ReaderConfig.MultiConfig, s.PipelineInfo().EventPool, s.productFunc)
		s.multilineProcessor.StartTask(s.mTask)
		s.productFunc = s.multilineProcessor.Process
	}
	if s.config.AckConfig.Enable {
		s.ackTask = NewAckTask(s.Epoch(), s.PipelineName(), s.Name(), func(state *State) {
			s.dbHandler.state <- state
		})
		s.ackChainHandler.StartTask(s.ackTask)
	}
	s.watchTask = NewWatchTask(s.Epoch(), s.PipelineName(), s.Name(), s.config.CollectConfig, s.PipelineInfo().EventPool, s.productFunc, s.r.jobChan)
	// start watch source paths
	s.watcher.StartWatchTask(s.watchTask)
}
