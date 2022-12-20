package journal

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	journalctl "github.com/loggie-io/loggie/pkg/source/journal/ctl"
	"github.com/loggie-io/loggie/pkg/util/persistence"
)

const (
	Type           = "journal"
	SystemStateKey = event.SystemKeyPrefix + "State"
	timeFmt        = "2006-01-02 15:04:05"

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
	return nil
}

func (s *Source) Stop() {
	close(s.done)
}

func (s *Source) Start() error {
	s.cmd = journalctl.NewJournalCtlCmd()
	now := time.Now()
	registry := s.findExistRegistry()
	if len(s.config.StartTime) > 0 {
		s.startTime, _ = time.ParseInLocation(timeFmt, s.config.StartTime, time.Local)
		if now.Sub(s.startTime) > time.Hour*(24*3) {
			log.Warn("duration too long")
			return errors.New("duration too long")
		}
		lastOffset := registry.Offset
		if lastOffset > s.startTime.UnixMicro() {
			s.startTime = time.UnixMicro(lastOffset)
		}
	} else {
		s.startTime = now
	}

	s.toCollectTime = s.startTime.Add(time.Hour)
	if registry.Id == 0 {
		s.preAllocationOffset()
	}

	return nil
}

func (s *Source) findExistRegistry() persistence.Registry {
	return s.dbHandler.FindBy(s.watchId, s.name, s.pipelineName)
}

func (s *Source) ProductLoop(productFunc api.ProductFunc) {
	log.Info("collect from %s", s.startTime.Format(timeFmt))
	s.startCollectHistory(productFunc)
	ticker := time.NewTicker(time.Duration(s.config.CollectInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.toCollectTime = time.Now()
			err := s.collect(s.config.Dir, s.config.Unit, s.startTime, s.toCollectTime, s.cmd, productFunc)
			if err != nil {
				log.Warn("collect journal logs failed: %s", err.Error())
			}
			s.startTime = s.toCollectTime
		}
	}
}

func (s *Source) Commit(events []api.Event) {
	sort.Sort(sortableEvents(events))
	s.dbHandler.State <- getState(events[0])
	s.eventPool.PutAll(events)
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
				log.Info("time to collect %s is after %s", s.toCollectTime.Format(timeFmt), time.Now().Format(timeFmt))
				s.toCollectTime = time.Now()
				historyCollected = true
			}

			start := time.Now()
			err := s.collect(s.config.Dir, s.config.Unit, s.startTime, s.toCollectTime, s.cmd, productFunc)
			d := time.Now().Sub(start).Seconds()
			log.Info("collect using %f s", d)

			if err != nil {
				log.Warn("collect journal logs failed: %s", err.Error())
			}

			if historyCollected {
				select {
				case s.historyDone <- struct{}{}:
					log.Info("history log collect finished")
				default:
				}
				return
			}

			s.startTime = s.toCollectTime
			s.toCollectTime = s.toCollectTime.Add(collectDuration)
		}

	}

}

func (s *Source) collect(dir, unit string, since, until time.Time, cmd *journalctl.Command, productFunc api.ProductFunc) error {
	log.Debug("going to collect logs from %s to %s", since.Format(timeFmt), until.Format(timeFmt))
	cmd.Clear()
	if len(unit) > 0 {
		cmd.WithUnit(unit)
	}
	cmd.WithDir(dir).WithSince(since.Format(timeFmt)).WithUntil(until.Format(timeFmt)).WithOutputFormat("json").WithNoPager()
	bs, err := cmd.RunCmd()
	if err != nil {
		log.Warn("run cmd failed")
		return err
	}

	s.processEvents(bs, productFunc)

	log.Debug("collected logs from %s to %s", since.Format(timeFmt), until.Format(timeFmt))

	return nil
}

func (s *Source) processEvents(bs []byte, productFunc api.ProductFunc) {

	sc := bufio.NewScanner(bytes.NewReader(bs))
	for sc.Scan() {
		logBody := map[string]string{}
		logBytes := sc.Bytes()
		err := json.Unmarshal(logBytes, &logBody)
		if err != nil {
			log.Warn("fail to decode journal body")
			continue
		}

		e := s.eventPool.Get()
		for k, v := range logBody {
			e.Header()[k] = v
		}
		e.Fill(e.Meta(), e.Header(), []byte(logBody[JMessage]))
		timeStamp := logBody[JTimestamp]
		var offset int64
		atoi, err := strconv.Atoi(timeStamp)
		if err != nil {
			log.Warn("fail to get timestamp from journal log")
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
		e.Meta().Set(SystemStateKey, state)
		productFunc(e)
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
}

func getState(e api.Event) *persistence.State {
	if e == nil {
		panic("event is nil")
	}
	state, _ := e.Meta().Get(SystemStateKey)
	return state.(*persistence.State)
}
