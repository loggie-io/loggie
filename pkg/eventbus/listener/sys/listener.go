package sys

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/eventbus/export/logger"
	promeExporter "github.com/loggie-io/loggie/pkg/eventbus/export/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/v3/process"
)

const name = "sys"

func init() {
	eventbus.Registry(name, makeListener, eventbus.WithTopic(eventbus.SystemTopic))
}

func makeListener() eventbus.Listener {
	l := &Listener{
		done:   make(chan struct{}),
		data:   sysData{},
		config: &Config{},
	}
	return l
}

type sysData struct {
	MemoryRss  uint64  `json:"memRss"`
	CPUPercent float64 `json:"cpuPercent"`
}

type Config struct {
	Period time.Duration `yaml:"period" default:"10s"`
}

type Listener struct {
	config *Config
	proc   *process.Process
	data   sysData
	done   chan struct{}
}

func (l *Listener) Name() string {
	return name
}

func (l *Listener) Init(ctx api.Context) error {
	pid := os.Getpid()
	proc, err := process.NewProcess(int32(pid))
	if err != nil {
		log.Fatal("get process err %v", err)
	}

	l.proc = proc
	return nil
}

func (l *Listener) Start() error {
	go l.export()
	return nil
}

func (l *Listener) Stop() {
	close(l.done)
}

func (l *Listener) Subscribe(event eventbus.Event) {
	// Do nothing
}

func (l *Listener) Config() interface{} {
	return l.config
}

func (l *Listener) export() {
	tick := time.NewTicker(l.config.Period)
	defer tick.Stop()
	for {
		select {
		case <-l.done:
			return
		case <-tick.C:
			if err := l.getSysStat(); err != nil {
				log.Error("get system stat failed: %v", err)
				return
			}

			l.exportPrometheus()
			m, _ := json.Marshal(l.data)
			logger.Export(eventbus.SystemTopic, m)
		}
	}
}

func (l *Listener) getSysStat() error {

	mem, err := l.proc.MemoryInfo()
	if err != nil {
		return err
	}
	l.data.MemoryRss = mem.RSS

	cpuPer, err := l.proc.Percent(1 * time.Second)
	if err != nil {
		return err
	}
	if pruneCPU, err := strconv.ParseFloat(fmt.Sprintf("%.2f", cpuPer), 64); err == nil {
		cpuPer = pruneCPU
	}
	l.data.CPUPercent = cpuPer

	return nil
}

func (l *Listener) exportPrometheus() {
	metric := promeExporter.ExportedMetrics{
		{
			Desc: prometheus.NewDesc(
				prometheus.BuildFQName(promeExporter.Loggie, eventbus.SystemTopic, "mem_rss"),
				"Loggie memory rss bytes",
				nil, nil,
			),
			Eval:    float64(l.data.MemoryRss),
			ValType: prometheus.GaugeValue,
		},
		{
			Desc: prometheus.NewDesc(
				prometheus.BuildFQName(promeExporter.Loggie, eventbus.SystemTopic, "cpu_percent"),
				"Loggie cpu percent",
				nil, nil,
			),
			Eval:    l.data.CPUPercent,
			ValType: prometheus.GaugeValue,
		},
	}
	promeExporter.Export(eventbus.SystemTopic, metric)
}
