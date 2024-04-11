package stream

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/filestream/config"
	"github.com/loggie-io/loggie/pkg/source/filestream/monitor"
	define "github.com/loggie-io/loggie/pkg/source/filestream/process/define"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
	"sync/atomic"
)

type Collector struct {
	status       CollectorStatus
	path         string
	progress     storage.CollectProgress
	streamList   FilestreamList
	streamName   string
	processChain define.ChainProcess
	monitor      *monitor.Monitor
	running      uint32
}

// NewStreamCollector 初始化流采集器
func NewStreamCollector(config *config.InputConfig, progress storage.CollectProgress, processor define.ChainProcess,
	monitor *monitor.Monitor) *Collector {
	return &Collector{
		path:         config.FilePath,
		streamName:   config.StreamName,
		progress:     progress,
		streamList:   NewFilesPriorityQueue(config.FilePath, config.StreamMaxSize, &config.StreamFileConfig, progress),
		processChain: processor,
		monitor:      monitor,
		running:      1,
	}
}

// Init 初始化流列表
func (c *Collector) Init() error {
	begin := c.monitor.BeginTime()
	defer c.monitor.CountScan(begin, c.streamList.GetListLen())
	return c.streamList.Init()
}

// Scan 采集
func (c *Collector) Scan() (bool, error) {
	if c.status.IsRun() {
		log.Info("%s collector have run", c.streamName)
		return false, nil
	}

	if atomic.LoadUint32(&c.running) == 0 {
		return false, nil
	}

	begin := c.monitor.BeginTime()
	defer c.monitor.CountScan(begin, c.streamList.GetListLen())
	err := c.streamList.Init()
	if err != nil {
		log.Error("s.streamList.Init error:%s", err)
		return false, nil
	}

	err = c.streamList.HasScroll()

	if err == HasChange {
		if err != nil {
			log.Error("s.streamList.Init error:%s", err)
			return false, nil
		}
		return true, nil
	}

	if err != nil {
		log.Error("s.streamList.Init error:%s", err)
		return false, err
	}

	finish, err := c.streamList.IsFinish()

	if err != nil {
		log.Error("s.streamList.Init error:%s", err)
		return false, err
	}

	// ZL: 采集完了这个流已经
	if finish == true {
		log.Info("%s collector have finish", c.streamName)
		return false, nil
	}

	return true, nil
}

// Collect 采集
func (c *Collector) Collect() (bool, error) {
	defer c.status.Finish()
	if c.status.Do() == false {
		log.Error("%s collector have run")
		return false, nil
	}

	if atomic.LoadUint32(&c.running) == 0 {
		return false, nil
	}

	// 收集文件
	file, err := c.streamList.Select()
	if err != nil {
		return false, err
	}

	if file == nil {
		return true, nil
	}

	// 开始采集文件
	file.Collect(c.processChain, c)

	return true, nil
}

// Stop 停止采集器采集
func (c *Collector) Stop() {
	log.Info("Collector Stopping")
	atomic.StoreUint32(&c.running, 0)
	c.streamList.Stop()
}
