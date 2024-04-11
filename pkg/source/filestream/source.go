package filestream

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/source/filestream/ack"
	fileStreamConfig "github.com/loggie-io/loggie/pkg/source/filestream/config"
	"github.com/loggie-io/loggie/pkg/source/filestream/monitor"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
	"github.com/pkg/errors"
)

const Type = "filestream"

type Source struct {
	pipelineName    string
	epoch           *pipeline.Epoch
	rc              *pipeline.RegisterCenter
	eventPool       *event.Pool
	sinkCount       int
	ackEnable       bool
	name            string
	out             chan api.Event
	config          *fileStreamConfig.Config
	producer        *Producer
	ackChainHandler *ack.ChainHandler
	reporter        *monitor.MetricReporter
	ackTask         *ack.Task
}

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	ptr := &Source{
		pipelineName: info.PipelineName,
		epoch:        info.Epoch,
		rc:           info.R,
		eventPool:    info.EventPool,
		sinkCount:    info.SinkCount,
		config:       &fileStreamConfig.Config{},
	}
	return ptr
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

func (s *Source) Config() interface{} {
	return s.config
}

func (s *Source) Init(context api.Context) error {
	if len(s.config.Inputs) == 0 {
		return errors.New(fmt.Sprintf("inputs is 0;pipeline:%s", s.pipelineName))
	}

	s.name = context.Name()
	s.out = make(chan api.Event, s.sinkCount)

	// 初始化streamName
	for _, inputPtr := range s.config.Inputs {
		inputPtr.StreamName = fmt.Sprintf("%s-%s-%s", s.pipelineName, s.name, inputPtr.StreamName)
		log.Info("%s init", inputPtr.StreamName)
	}
	s.ackEnable = *s.config.AckConfig.Enable
	// 初始化全局处理的池子
	InitWorkerPool()
	// 初始化存储实例
	storage.GetOrCreateKeeper()
	// 初始化指标上报器
	s.reporter = monitor.NewMetricReporter(s.pipelineName, s.name)
	// 初始化生产者
	s.producer = NewProducer(s, s.reporter)
	return nil
}

func (s *Source) Start() error {
	if s.ackEnable {
		s.ackChainHandler = ack.GetOrCreateShareAckChainHandler(s.sinkCount, s.config.AckConfig)
		s.rc.RegisterListener(ack.NewListener(s.name, s.ackChainHandler))

	}
	return nil
}

func (s *Source) Stop() {
	if s.ackEnable {
		log.Info("s.ackChainHandler.StopTask start :%v", s)
		s.ackChainHandler.StopTask(s.ackTask)
		log.Info("[%s] all ack jobs of source exit", s.String())
	}

	// 停止生产
	s.producer.Stop(s.config)

	//停止metrics上报
	s.reporter.StopMetric()
	return
}

func (s *Source) ProductLoop(productFunc api.ProductFunc) {
	if *s.config.AckConfig.Enable {
		ackMonitor := monitor.NewAckMonitor(s.reporter, s.config.Fields)
		log.Info("[%s] Ack Start", s.String())
		s.ackTask = ack.NewAckTask(s.epoch, s.pipelineName, s.name, ackMonitor)
		s.ackChainHandler.StartTask(s.ackTask)
	}

	s.producer.Start(s.reporter, productFunc)

	return
}

func (s *Source) Commit(events []api.Event) {
	// ack events
	if s.ackEnable {
		marks := make([]ack.Mark, 0, len(events))
		for _, e := range events {
			mark := ack.GetAckMark(e)
			if mark == nil {
				log.Error("ack mark is nil:%v", mark)
				continue
			}
			marks = append(marks, mark)
		}
		s.ackChainHandler.AckChan <- marks
	}
	// release events
	s.eventPool.PutAll(events)

	return
}
