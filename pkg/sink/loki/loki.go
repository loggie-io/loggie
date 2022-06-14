package loki

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/loggie-io/loggie/pkg/core/api"
	eventer "github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/global"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/loki/logproto"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/pkg/errors"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"golang.org/x/net/context"
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

func (s *Sink) Init(context api.Context) error {
	s.name = context.Name()

	cli, err := config.NewClientFromConfig(config.DefaultHTTPClientConfig, "loggie")
	if err != nil {
		log.Error("init loki default client failed: %v", err)
		return nil
	}

	s.client = cli
	return nil
}

func (s *Sink) Start() error {
	log.Info("%s started", s.String())

	return nil
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
		stream, err := s.event2stream(event)
		if err != nil {
			return result.Drop().WithError(errors.WithMessage(err, "convert event to loki stream error"))
		}

		streams = append(streams, *stream)
	}

	var req *http.Request
	var err error
	if s.config.ContentType == "json" {
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

const token = "_"

func (s *Sink) event2stream(event api.Event) (*logproto.Stream, error) {
	t, ok := event.Meta().Get(eventer.SystemProductTimeKey)
	if !ok {
		t = time.Now()
	}

	obj := runtime.NewObject(event.Header())
	flatHeader, err := obj.FlatKeyValue(token)
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
		labelSet[model.LabelName("loggie_host")] = model.LabelValue(global.NodeName)
	}

	var line string
	if s.config.EntryLine == "" {
		line = string(event.Body())
	} else {
		str, err := obj.GetPath(s.config.EntryLine).String()
		if err != nil {
			return nil, errors.WithMessage(err, "get lineKey from event failed")
		}
		line = str
	}

	stream := logproto.Stream{
		Labels: labelSet.String(),
		Entries: []logproto.Entry{
			{
				Timestamp: t.(time.Time),
				Line:      line,
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
