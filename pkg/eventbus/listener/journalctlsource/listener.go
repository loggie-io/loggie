package journalctlsource

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/eventbus/export/logger"
	promeExporter "github.com/loggie-io/loggie/pkg/eventbus/export/prometheus"
	"github.com/loggie-io/loggie/pkg/source/journal"
)

const name = "journalsource"

func init() {
	eventbus.Registry(name, makeListener, eventbus.WithTopics([]string{eventbus.JournalSourceMetricTopic}))
}

func makeListener() eventbus.Listener {
	l := &Listener{
		eventChan: make(chan eventbus.JournalMetricData),
		done:      make(chan struct{}),
		data:      make(map[string]*data),
		config:    &Config{},
	}
	return l
}

type Config struct {
	Period time.Duration `yaml:"period" default:"10s"`
}

type Listener struct {
	config    *Config
	done      chan struct{}
	eventChan chan eventbus.JournalMetricData
	data      map[string]*data // key=pipelineName+sourceName
}

type data struct {
	PipelineName  string `json:"pipeline"`
	SourceName    string `json:"source"`
	Offset        int64  `json:"-"`
	CollectedTime string `json:"collectOffset"`
	AckOffset     int64  `json:"-"`
	AckTime       string `json:"ackOffset"`
}

func (l *Listener) Name() string {
	return name
}

func (l *Listener) Init(context api.Context) error {
	return nil
}

func (l *Listener) Start() error {
	go l.run()
	return nil
}

func (l *Listener) Stop() {
	close(l.done)
}

func (l *Listener) Config() interface{} {
	return l.config
}

func (l *Listener) Subscribe(event eventbus.Event) {
	e, ok := event.Data.(eventbus.JournalMetricData)
	if !ok {
		log.Panic("type assert eventbus.JournalMetricData failed: %v", ok)
	}
	l.eventChan <- e
}

func (l *Listener) run() {
	tick := time.NewTicker(l.config.Period)
	defer tick.Stop()
	for {
		select {
		case <-l.done:
			return

		case e := <-l.eventChan:
			l.consumer(e)

		case <-tick.C:
			l.compute()
			l.exportPrometheus()

			m, _ := json.Marshal(l.data)
			logger.Export(eventbus.JournalSourceMetricTopic, m)

			l.clean()
		}
	}
}

func (l *Listener) compute() {
	for _, metric := range l.data {
		metric.CollectedTime = time.UnixMicro(metric.Offset).Format(journal.TimeFmt)
		metric.AckTime = time.UnixMicro(metric.AckOffset).Format(journal.TimeFmt)
	}
}

func (l *Listener) consumer(e eventbus.JournalMetricData) {
	var buf strings.Builder
	buf.WriteString(e.PipelineName)
	buf.WriteString("-")
	buf.WriteString(e.SourceName)
	key := buf.String()

	metric, ok := l.data[key]
	if !ok {
		d := &data{
			PipelineName: e.PipelineName,
			SourceName:   e.SourceName,
		}

		if e.Type == eventbus.JournalCollectOffset {
			d.Offset = e.Offset
		}

		if e.Type == eventbus.JournalAckOffset {
			d.AckOffset = e.AckOffset
		}

		l.data[key] = d
		return
	}

	if e.Type == eventbus.JournalCollectOffset && e.Offset > metric.Offset {
		metric.Offset = e.Offset
	}

	if e.Type == eventbus.JournalAckOffset && e.AckOffset > metric.AckOffset {
		metric.AckOffset = e.AckOffset
	}
}

func (l *Listener) clean() {

}

func (l *Listener) exportPrometheus() {

	metrics := promeExporter.ExportedMetrics{}
	for _, d := range l.data {
		m := promeExporter.ExportedMetrics{
			{
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(promeExporter.Loggie, eventbus.JournalSourceMetricTopic, "collect_offset"),
					"journalctl collect offset",
					nil, prometheus.Labels{promeExporter.PipelineNameKey: d.PipelineName, promeExporter.SourceNameKey: d.SourceName},
				),
				Eval:    float64(d.Offset),
				ValType: prometheus.GaugeValue,
			},
			{
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(promeExporter.Loggie, eventbus.JournalSourceMetricTopic, "ack_offset"),
					"sink ack offset",
					nil, prometheus.Labels{promeExporter.PipelineNameKey: d.PipelineName, promeExporter.SourceNameKey: d.SourceName},
				),
				Eval:    float64(d.AckOffset),
				ValType: prometheus.GaugeValue,
			},
		}

		metrics = append(metrics, m...)
	}
	promeExporter.Export(eventbus.SinkMetricTopic, metrics)
}
