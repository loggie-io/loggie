package prometheus_exporter

import (
	ctx "context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prom2json"
	"io"
	"io/ioutil"
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/event"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/pipeline"
	"loggie.io/loggie/pkg/util"
	"net/http"
	"strconv"
	"time"
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
	name        string
	config      *Config
	client      *http.Client
	requestPool []*http.Request
	done        chan struct{}
	eventPool   *event.Pool
}

func (k *PromExporter) Config() interface{} {
	return k.config
}

func (k *PromExporter) Category() api.Category {
	return api.SOURCE
}

func (k *PromExporter) Type() api.Type {
	return Type
}

func (k *PromExporter) String() string {
	return fmt.Sprintf("%s/%s", api.SOURCE, Type)
}

const acceptHeader = `application/openmetrics-text; version=0.0.1,text/plain;version=0.0.4;q=0.5,*/*;q=0.1`

func (k *PromExporter) Init(context api.Context) {
	k.name = context.Name()

	k.client = &http.Client{}
	for _, ep := range k.config.Endpoints {
		req, err := http.NewRequest(http.MethodGet, ep, nil)
		if err != nil {
			log.Warn("request to endpoint %s error: %v", ep, err)
			continue
		}

		req.Header.Add("Accept", acceptHeader)
		req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", strconv.FormatFloat(k.config.Timeout.Seconds(), 'f', -1, 64))

		k.requestPool = append(k.requestPool, req)
	}
}

func (k *PromExporter) Start() {
}

func (k *PromExporter) Stop() {
	log.Info("stopping source prometheusExporter: %s", k.name)
	close(k.done)
}

func (k *PromExporter) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", k.String())

	t := time.NewTicker(k.config.Interval)
	defer t.Stop()

	background, cancel := ctx.WithCancel(ctx.Background())
	defer cancel()
	for {
		select {
		case <-k.done:
			return

		case <-t.C:
			k.batchScrape(background, productFunc)
		}
	}
}

func (k *PromExporter) Commit(events []api.Event) {
	k.eventPool.PutAll(events)
}

func (k *PromExporter) batchScrape(c ctx.Context, productFunc api.ProductFunc) {
	for _, req := range k.requestPool {
		metrics, err := k.scrape(c, req)
		if err != nil {
			log.Warn("request to exporter error: %+v", err)
			continue
		}

		e := k.eventPool.Get()
		e.Fill(e.Meta(), e.Header(), metrics)

		productFunc(e)
	}
}

func (k *PromExporter) scrape(c ctx.Context, req *http.Request) ([]byte, error) {
	ct, cancel := ctx.WithTimeout(c, k.config.Timeout)
	defer cancel()
	resp, err := k.client.Do(req.WithContext(ct))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("server returned HTTP status %s", resp.Status)
	}

	if k.config.ToJson {
		out, err := promToJson(resp.Body)
		if err != nil {
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

func promToJson(in io.Reader) ([]byte, error) {
	var parser expfmt.TextParser
	metricFamilies, err := parser.TextToMetricFamilies(in)
	if err != nil {
		return nil, errors.WithMessage(err, "reading text format failed")
	}

	family := make(map[string]*prom2json.Family)
	for name, val := range metricFamilies {
		// add timestamp in metrics
		for _, m := range val.Metric {
			timeMs := util.UnixMilli(time.Now())
			m.TimestampMs = &timeMs
		}

		f := prom2json.NewFamily(val)
		family[name] = f
	}
	out, err := json.Marshal(family)
	if err != nil {
		return nil, errors.WithMessage(err, "json marshal prometheus metrics error")
	}

	return out, nil
}
