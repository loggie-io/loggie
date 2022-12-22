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

package journal

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/loggie-io/loggie/pkg/util/bufferpool"
	"sort"
	"strconv"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/pipeline"
	journalctl "github.com/loggie-io/loggie/pkg/source/journal/ctl"
	"github.com/loggie-io/loggie/pkg/util/persistence"
)

const (
	Type           = "journal"
	SystemStateKey = event.SystemKeyPrefix + "State"
	TimeFmt        = "2006-01-02 15:04:05"

	longestDuration = time.Hour * 24 * 3

	JMessage   = "MESSAGE"
	JTimestamp = "__REALTIME_TIMESTAMP"
)

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	return &Source{
		pipelineName: info.PipelineName,
		done:         make(chan struct{}),
		config:       &Config{},
		eventPool:    info.EventPool,
	}
}

type Source struct {
	pipelineName  string
	name          string
	watchId       string
	done          chan struct{}
	historyDone   chan struct{}
	config        *Config
	eventPool     *event.Pool
	startTime     time.Time
	toCollectTime time.Time

	cmd *journalctl.Command
	bp  *bufferpool.BufferPool

	dbHandler *persistence.DbHandler
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
	s.watchId = s.pipelineName + "-" + s.name
	s.dbHandler = persistence.GetOrCreateShareDbHandler(s.config.DbConfig)
	s.bp = bufferpool.NewBufferPool(1024)
	return nil
}

func (s *Source) Stop() {
	close(s.done)
}

func (s *Source) Start() error {
	s.cmd = journalctl.NewJournalCtlCmd()
	now := time.Now()
	registry := s.findExistRegistry()
	if registry.Offset == 0 {
		s.preAllocationOffset()
		if len(s.config.StartTime) > 0 {
			s.startTime, _ = time.ParseInLocation(TimeFmt, s.config.StartTime, time.Local)
			if now.Sub(s.startTime) > longestDuration {
				log.Warn("duration too long")
				return errors.New("duration too long")
			}
		} else {
			s.startTime = now
		}
	} else {
		s.startTime = time.UnixMicro(registry.Offset)
		if now.Sub(s.startTime) > longestDuration {
			s.startTime = now.Add(longestDuration * -1)
			log.Info("%s last collect time too far away", s.watchId)
		}
	}

	s.toCollectTime = s.startTime.Add(s.config.HistorySplitDuration)

	return nil
}

func (s *Source) findExistRegistry() persistence.Registry {
	return s.dbHandler.FindBy(s.watchId, s.name, s.pipelineName)
}

func (s *Source) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s collect from %s", s.watchId, s.startTime.Format(TimeFmt))
	s.startCollectHistory(productFunc)
	ticker := time.NewTicker(time.Duration(s.config.CollectInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.toCollectTime = time.Now()
			err := s.collect(s.config.Dir, s.config.Unit, s.config.Identifier, s.startTime, s.toCollectTime, s.cmd, productFunc)
			if err != nil {
				log.Warn("%s collect journal logs failed: %s", s.watchId, err.Error())
			}
			s.startTime = s.toCollectTime
		}
	}
}

func (s *Source) Commit(events []api.Event) {
	sort.Sort(sortableEvents(events))
	state := getState(events[0])
	s.dbHandler.State <- state
	s.eventPool.PutAll(events)
	log.Debug("%s commit journal log event offset %d", s.watchId, state.Offset)
	if state.Offset > 0 {
		eventbus.PublishOrDrop(eventbus.JournalSourceMetricTopic, eventbus.JournalMetricData{
			PipelineName: s.pipelineName,
			SourceName:   s.name,
			Offset:       0,
			AckOffset:    state.Offset,
			Type:         eventbus.JournalAckOffset,
		})
	}
}

func (s *Source) startCollectHistory(productFunc api.ProductFunc) {
	s.historyDone = make(chan struct{})
	go s.collectHistory(productFunc)
	for {
		select {
		case <-s.done:
			return
		case <-s.historyDone:
			return
		}
	}
}

func (s *Source) collectHistory(productFunc api.ProductFunc) {
	collectDuration := s.config.HistorySplitDuration
	historyCollected := false
	for {
		select {
		case <-s.done:
			return
		default:
			if s.toCollectTime.After(time.Now()) {
				log.Info("%s time to collect %s is after %s", s.watchId, s.toCollectTime.Format(TimeFmt), time.Now().Format(TimeFmt))
				s.toCollectTime = time.Now()
				historyCollected = true
			}

			start := time.Now()
			err := s.collect(s.config.Dir, s.config.Unit, s.config.Identifier, s.startTime, s.toCollectTime, s.cmd, productFunc)
			d := time.Now().Sub(start).Seconds()
			log.Info("%s collect using %f s", s.watchId, d)

			if err != nil {
				log.Warn("%s collect journal logs failed: %s", s.watchId, err.Error())
			}

			if historyCollected {
				select {
				case s.historyDone <- struct{}{}:
					log.Info("%s history log collect finished", s.watchId)
				default:
				}
				return
			}

			s.startTime = s.toCollectTime
			s.toCollectTime = s.toCollectTime.Add(collectDuration)
		}

	}

}

func (s *Source) collect(dir, unit, target string, since, until time.Time, cmd *journalctl.Command, productFunc api.ProductFunc) error {
	sinceFormat := since.Format(TimeFmt)
	untilFormat := until.Format(TimeFmt)

	log.Debug("%s going to collect logs from %s to %s", s.watchId, sinceFormat, untilFormat)
	cmd.Clear()
	if len(unit) > 0 {
		cmd.WithUnit(unit)
	}
	if len(target) > 0 {
		cmd.WithIdentifier(target)
	}
	cmd.WithDir(dir).WithSince(sinceFormat).WithUntil(untilFormat).WithOutputFormat("json").WithNoPager()
	buffer := s.bp.Get()
	bs, err := cmd.RunCmd(buffer)
	defer s.bp.Put(buffer)
	if err != nil {
		log.Warn("%s run cmd failed: %s", s.watchId, err.Error())
		return err
	}

	s.processEvents(bs, productFunc)
	log.Debug("%s collected logs from %s to %s", s.watchId, sinceFormat, untilFormat)

	return nil
}

func (s *Source) processEvents(bs []byte, productFunc api.ProductFunc) {

	sc := bufio.NewScanner(bytes.NewReader(bs))
	var lasted int64
	for sc.Scan() {
		logBody := map[string]string{}
		logBytes := sc.Bytes()
		err := json.Unmarshal(logBytes, &logBody)
		if err != nil {
			log.Warn("fail to decode journal body: %s", err.Error())
			continue
		}

		e := s.eventPool.Get()
		if s.config.AddAllMeta {
			for k, v := range logBody {
				e.Header()[k] = v
			}
		} else {
			for k, v := range s.config.AddMeta {
				e.Header()[k] = logBody[v]
			}
		}

		e.Fill(e.Meta(), e.Header(), []byte(logBody[JMessage]))
		timeStamp := logBody[JTimestamp]
		var offset int64
		atoi, err := strconv.Atoi(timeStamp)
		if err != nil {
			log.Warn("fail to get timestamp from journal log: %s", err.Error())
			offset = s.startTime.UnixMicro()
		} else {
			offset = int64(atoi)
		}
		state := &persistence.State{
			PipelineName: s.pipelineName,
			SourceName:   s.name,
			Offset:       offset,
			NextOffset:   offset,
			Filename:     s.watchId,
			CollectTime:  time.Now(),
			JobUid:       s.watchId,
			WatchUid:     s.watchId,
		}
		if offset > lasted {
			lasted = offset
		}
		e.Meta().Set(SystemStateKey, state)
		productFunc(e)
	}

	if lasted > 0 {
		eventbus.PublishOrDrop(eventbus.JournalSourceMetricTopic, eventbus.JournalMetricData{
			PipelineName: s.pipelineName,
			SourceName:   s.name,
			Offset:       lasted,
			AckOffset:    0,
			Type:         eventbus.JournalCollectOffset,
		})
	}
}

func (s *Source) preAllocationOffset() {
	s.dbHandler.HandleOpt(persistence.DbOpt{
		R: persistence.Registry{
			PipelineName: s.pipelineName,
			SourceName:   s.name,
			Filename:     s.watchId,
			JobUid:       s.watchId,
			Offset:       s.startTime.UnixMicro(),
		},
		OptType:     persistence.UpsertOffsetByJobWatchIdOpt,
		Immediately: true,
	})
	log.Debug("%s preAllocationOffset", s.watchId)
}

func getState(e api.Event) *persistence.State {
	if e == nil {
		panic("event is nil")
	}
	state, _ := e.Meta().Get(SystemStateKey)
	return state.(*persistence.State)
}
