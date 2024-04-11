package filestream

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/global"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	config2 "github.com/loggie-io/loggie/pkg/source/filestream/config"
	"github.com/loggie-io/loggie/pkg/source/filestream/monitor"
	"github.com/loggie-io/loggie/pkg/source/filestream/process"
	"github.com/loggie-io/loggie/pkg/source/filestream/stream"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"
)

type Producer struct {
	ProductFunc  api.ProductFunc
	Fields       map[string]interface{}
	Epoch        *pipeline.Epoch
	Reporter     *monitor.MetricReporter
	AckEnable    bool
	PipelineName string
	SourceName   string
	running      int32
	wgRun        sync.WaitGroup
	done         chan struct{}
	config       *config2.Config
	meta         map[string]interface{}
	collectors   map[string]*stream.Collector
	eventPool    *event.Pool
	ackEnable    bool
}

func NewProducer(source *Source, reporter *monitor.MetricReporter) *Producer {
	meta := make(map[string]interface{}, 3)
	meta["module"] = "loggie"
	meta["type"] = ""
	meta["version"] = global.GetVersion()
	collectors := make(map[string]*stream.Collector)

	return &Producer{
		Epoch:        source.epoch,
		Fields:       source.config.Fields,
		Reporter:     reporter,
		AckEnable:    *source.config.AckConfig.Enable,
		PipelineName: source.pipelineName,
		SourceName:   source.name,
		running:      0,
		done:         make(chan struct{}),
		meta:         meta,
		config:       source.config,
		collectors:   collectors,
		ackEnable:    source.ackEnable,
		eventPool:    source.eventPool,
	}
}

// scan 对流进行扫描，成功后提交到事件池
func (producer *Producer) scan(collector *stream.Collector) (sub bool, err error) {
	// 已经停止了
	if atomic.LoadInt32(&producer.running) == 0 {
		return
	}

	if err != nil {
		return
	}

	sub, err = collector.Scan()

	if sub == false {
		return
	}

	if err != nil {
		log.Error("%s scan error:%s", producer.SourceName, err)
		return
	}

	GlobalWorkerPool.Submit(func() {
		collector.Collect()
	})
	return
}

func (producer *Producer) StreamCheckingLoop() {
	producer.wgRun.Add(1)
	defer func() {
		producer.wgRun.Done()
		log.Info("StreamCheckingLoop End Defer")
	}()

	cntSleep := 1
	timer := time.NewTicker(time.Duration(cntSleep) * time.Second)
	for {
		select {
		case <-producer.done:
			log.Info("%s producer.done", producer.SourceName)
			return
		case <-timer.C:
			var numSubmitted int

			log.Info("timer ticker")
			if atomic.LoadInt32(&producer.running) != 1 {
				log.Info("producer running == 0")
				return
			}
			for _, inpCfg := range producer.config.Inputs {
				if atomic.LoadInt32(&producer.running) != 1 {
					log.Info("producer running == 0")
					return
				}

				collector, ok := producer.collectors[inpCfg.StreamName]
				if ok == false {
					log.Error("producer.collectors is not exist %s", inpCfg.StreamName)
					continue
				}

				sub, err := producer.scan(collector)
				if err != nil {
					log.Error("checkStream", zap.Error(err), zap.String("stream", inpCfg.StreamName))
				}
				if sub {
					numSubmitted++
				}
			}

			if numSubmitted > 0 {
				cntSleep = 1
			} else {
				cntSleep *= 2
				if cntSleep >= producer.config.MaxWaitCount {
					cntSleep = producer.config.MaxWaitCount
				}
			}

			log.Info("Sleep %v", producer.config.MaxWaitInterval)
			timer.Reset(time.Duration(cntSleep) * producer.config.MaxWaitInterval)
		}
	}

}

// Stop 通知Producer
func (producer *Producer) Stop(cfg *config2.Config) {
	atomic.StoreInt32(&producer.running, 0)
	close(producer.done)
	log.Info("StreamCheckingLoop close producer.done")
	producer.wgRun.Wait()
	log.Info("StreamCheckingLoop quit")
	// 停止采集器
	for _, collect := range producer.collectors {
		collect.Stop()
	}
}

// Start 启动Producer
func (producer *Producer) Start(export *monitor.MetricReporter, product api.ProductFunc) {
	atomic.StoreInt32(&producer.running, 1)
	producer.ProductFunc = product

	for _, streamCfg := range producer.config.Inputs {

		monitorExport := monitor.NewMonitor(export, streamCfg.StreamName, producer.Fields)

		// 初始化整个处理链条
		progress := stream.NewStreamProgress(streamCfg.StreamName)

		processorChain := process.NewChainProcessor(progress)

		processorChain.AddProcessor(process.NewLineProcessor(progress, &streamCfg.StreamProcessConfig.StreamMultiProcessConfig))

		processorChain.AddProcessor(process.NewDecoderProcessor(progress, &streamCfg.StreamProcessConfig.StreamDecoderProcessConfig))

		processorChain.AddProcessor(process.NewFirstPatternProcessor(progress, &streamCfg.StreamProcessConfig.StreamFirstPatternProcessConfig))

		processorChain.AddProcessor(process.NewProgressProcessor(progress))

		processorChain.AddProcessor(process.NewMarkProcessor(progress, producer.SourceName, producer.Epoch))

		processorChain.AddProcessor(process.NewTransporterProcessor(progress, producer.eventPool, product, producer.ackEnable))

		processorChain.AddProcessor(process.NewMonitorProcessor(monitorExport))

		// 初始化采集器
		collector := stream.NewStreamCollector(streamCfg, progress, processorChain, monitorExport)

		producer.collectors[streamCfg.StreamName] = collector
	}
	go producer.StreamCheckingLoop()
}
