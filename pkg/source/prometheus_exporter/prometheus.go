package prometheus_exporter

import (
	ctx "context"
	"encoding/json"
	"fmt"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/pkg/errors"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prom2json"
)

const Type = "prometheusExporter"

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	return &PromExporter{
		config:    &Config{},
		eventPool: info.EventPool,
		done:      make(chan struct{}),
	}
}

type PromExporter struct {
	name              string
	config            *Config
	client            *http.Client
	requestPool       []*http.Request
	done              chan struct{}
	eventPool         *event.Pool
	extraLabelsEnable bool
	labelPattern      map[string]*pattern.Pattern
}

func (e *PromExporter) Config() interface{} {
	return e.config
}

func (e *PromExporter) Category() api.Category {
	return api.SOURCE
}

func (e *PromExporter) Type() api.Type {
	return Type
}

func (e *PromExporter) String() string {
	return fmt.Sprintf("%s/%s", api.SOURCE, Type)
}

const acceptHeader = `application/openmetrics-text; version=0.0.1,text/plain;version=0.0.4;q=0.5,*/*;q=0.1`

func (e *PromExporter) Init(context api.Context) error {
	e.name = context.Name()

	e.client = &http.Client{}
	for _, ep := range e.config.Endpoints {
		req, err := http.NewRequest(http.MethodGet, ep, nil)
		if err != nil {
			log.Warn("request to endpoint %s error: %v", ep, err)
			continue
		}

		req.Header.Add("Accept", acceptHeader)
		req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", strconv.FormatFloat(e.config.Timeout.Seconds(), 'f', -1, 64))

		e.requestPool = append(e.requestPool, req)
	}

	if len(e.config.Labels) != 0 {
		e.extraLabelsEnable = true
		e.labelPattern = make(map[string]*pattern.Pattern)
		for key, val := range e.config.Labels {
			p, _ := pattern.Init(val)
			e.labelPattern[key] = p
		}
	}

	return nil
}

func (e *PromExporter) Start() error {
	return nil
}

func (e *PromExporter) Stop() {
	log.Info("stopping source prometheusExporter: %s", e.name)
	close(e.done)
}

func (e *PromExporter) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", e.String())

	t := time.NewTicker(e.config.Interval)
	defer t.Stop()

	background, cancel := ctx.WithCancel(ctx.Background())
	defer cancel()
	for {
		select {
		case <-e.done:
			return

		case <-t.C:
			e.batchScrape(background, productFunc)
		}
	}
}

func (e *PromExporter) Commit(events []api.Event) {
	e.eventPool.PutAll(events)
}

func (e *PromExporter) batchScrape(c ctx.Context, productFunc api.ProductFunc) {
	for _, req := range e.requestPool {
		metrics, err := e.scrape(c, req)
		if err != nil {
			log.Warn("request to exporter error: %+v", err)
			continue
		}

		e := e.eventPool.Get()
		e.Fill(e.Meta(), e.Header(), metrics)

		productFunc(e)
	}
}

func (e *PromExporter) scrape(c ctx.Context, req *http.Request) ([]byte, error) {
	ct, cancel := ctx.WithTimeout(c, e.config.Timeout)
	defer cancel()
	resp, err := e.client.Do(req.WithContext(ct))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("server returned HTTP status %s", resp.Status)
	}

	if e.config.ToJson {
		out, promErr := e.promToJson(resp.Body)
		if promErr != nil {
			return nil, errors.WithMessage(err, "convert prometheus metrics to json failed")
		}
		return out, nil
	}

	out, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.WithMessage(err, "read response body failed")
	}

	return out, nil
}

func (e *PromExporter) promToJson(in io.Reader) ([]byte, error) {
	var parser expfmt.TextParser
	metricFamilies, err := parser.TextToMetricFamilies(in)
	if err != nil {
		return nil, errors.WithMessage(err, "reading text format failed")
	}

	family := make([]*prom2json.Family, 0)
	for _, val := range metricFamilies {
		// add timestamp in metrics
		for _, m := range val.Metric {
			timeMs := util.UnixMilli(time.Now())
			m.TimestampMs = &timeMs
		}

		f := prom2json.NewFamily(val)

		if e.extraLabelsEnable {
			e.addLabels(f, e.config.Labels)
		}
		family = append(family, f)
	}
	out, err := json.Marshal(family)
	if err != nil {
		return nil, errors.WithMessage(err, "json marshal prometheus metrics error")
	}

	return out, nil
}

func (e *PromExporter) addLabels(family *prom2json.Family, labels map[string]string) {
	for key := range labels {
		val, err := e.labelPattern[key].Render()
		if err != nil {
			log.Warn("render label %s pattern failed: %v", key, err)
			continue
		}
		family.AddLabel(key, val)
	}
}
