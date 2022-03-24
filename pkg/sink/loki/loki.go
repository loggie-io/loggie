package loki

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/loggie-io/loggie/pkg/core/api"
	eventer "github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/core/sysconfig"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/loki/logproto"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/pkg/errors"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"golang.org/x/net/context"
	"io"
	"net/http"
	"time"
)

const Type = "loki"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Sink struct {
	name   string
	config *Config
	client *http.Client
}

func NewSink() *Sink {
	return &Sink{
		config: &Config{},
	}
}

func (s *Sink) Category() api.Category {
	return api.SINK
}

func (s *Sink) Type() api.Type {
	return Type
}

func (s *Sink) Config() interface{} {
	return s.config
}

func (s *Sink) String() string {
	return fmt.Sprintf("%s/%s", api.SINK, Type)
}

func (s *Sink) Init(context api.Context) {
	s.name = context.Name()

	cli, err := config.NewClientFromConfig(config.DefaultHTTPClientConfig, "loggie")
	if err != nil {
		log.Error("init loki default client failed: %v", err)
		return
	}

	s.client = cli
}

func (s *Sink) Start() {
	log.Info("%s started", s.String())
}

func (s *Sink) Stop() {
}

func (s *Sink) Consume(batch api.Batch) api.Result {
	if s.client == nil {
		return result.Drop() // FIXME fix this in issues(#117)
	}

	events := batch.Events()
	l := len(events)
	if l == 0 {
		return result.Success()
	}

	return s.sendBatch(context.Background(), batch)
}

func (s *Sink) sendBatch(c context.Context, batch api.Batch) api.Result {
	ctx, cancel := context.WithTimeout(c, s.config.Timeout)
	defer cancel()

	var streams []logproto.Stream
	for _, event := range batch.Events() {
		stream, err := event2stream(event)
		if err != nil {
			return result.Drop().WithError(errors.WithMessage(err, "convert event to loki stream error"))
		}

		streams = append(streams, *stream)
	}

	var req *http.Request
	var err error
	if s.config.EncodeJson {
		req, err = genJsonRequest(streams, s.config.URL, s.config.TenantId)
	} else {
		req, err = genProtoRequest(streams, s.config.URL, s.config.TenantId)
	}
	if err != nil {
		return result.Drop().WithError(err)
	}
	req = req.WithContext(ctx)

	resp, err := s.client.Do(req)
	if err != nil {
		return result.Fail(errors.WithMessage(err, "do http request failed when sending events to loki"))
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(resp.Body, 1024))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		return result.Fail(errors.Errorf("server returned HTTP status %s (%d): %s", resp.Status, resp.StatusCode, line))
	}

	return result.Success()
}

func event2stream(event api.Event) (*logproto.Stream, error) {
	t, ok := event.Meta().Get(eventer.SystemProductTimeKey)
	if !ok {
		t = time.Now()
	}

	obj := runtime.NewObject(event.Header())
	flatHeader, err := obj.FlatKeyValue()
	if err != nil {
		return nil, err
	}

	labelSet := model.LabelSet{}
	for k, v := range flatHeader {
		// we will ignore non-string value in header
		sv, ok := v.(string)
		if !ok {
			continue
		}

		labelSet[model.LabelName(k)] = model.LabelValue(sv)
	}

	// At least one label pair is required per stream in loki
	if len(labelSet) == 0 {
		labelSet[model.LabelName("loggie_host")] = model.LabelValue(sysconfig.NodeName)
	}

	stream := logproto.Stream{
		Labels: labelSet.String(),
		Entries: []logproto.Entry{
			{
				Timestamp: t.(time.Time),
				Line:      string(event.Body()), // TODO optimize: bytes to string
			},
		},
	}
	return &stream, nil
}

func genJsonRequest(streams []logproto.Stream, url string, tenantId string) (*http.Request, error) {
	pushReq := logproto.PushRequest{
		Streams: streams,
	}

	buf, err := json.Marshal(&pushReq)
	if err != nil {
		return nil, errors.WithMessage(err, "json.Marshal failed when sending events to loki")
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(buf))
	if err != nil {
		return nil, errors.WithMessage(err, "new http request failed when sending events to loki")
	}

	req.Header.Set("Content-Type", "application/json")

	if tenantId != "" {
		req.Header.Set("X-Scope-OrgID", tenantId)
	}

	return req, nil
}

func genProtoRequest(streams []logproto.Stream, url string, tenantId string) (*http.Request, error) {
	pushReq := logproto.PushRequest{
		Streams: streams,
	}

	buf, err := proto.Marshal(&pushReq)
	if err != nil {
		return nil, errors.WithMessage(err, "proto.Marshal failed when sending events to loki")
	}
	buf = snappy.Encode(nil, buf)

	req, err := http.NewRequest("POST", url, bytes.NewReader(buf))
	if err != nil {
		return nil, errors.WithMessage(err, "new http request failed when sending events to loki")
	}

	req.Header.Set("Content-Type", "application/x-protobuf")

	if tenantId != "" {
		req.Header.Set("X-Scope-OrgID", tenantId)
	}

	return req, nil
}
