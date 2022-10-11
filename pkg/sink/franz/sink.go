package franz

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/twmb/franz-go/pkg/kgo"
)

const Type = "franzKafka"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Sink struct {
	config *Config
	writer *kgo.Client
	cod    codec.Codec

	topicPattern *pattern.Pattern
}

func NewSink() *Sink {
	return &Sink{
		config: &Config{},
	}
}

func (s *Sink) Category() api.Category {
	return api.SINK
}

func (s *Sink) String() string {
	return fmt.Sprintf("%s/%s", api.SINK, Type)
}

func (s *Sink) Config() interface{} {
	return s.config
}

func (s *Sink) Type() api.Type {
	return Type
}

func (s *Sink) Init(context api.Context) error {
	s.topicPattern, _ = pattern.Init(s.config.Topic)
	return nil
}

func (s *Sink) SetCodec(c codec.Codec) {
	s.cod = c
}

func (s *Sink) Start() error {
	c := s.config

	c.convertKfkSecurity()
	// One client can both produce and consume!
	// Consuming can either be direct (no consumer group), or through a group. Below, we use a group.

	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ProducerBatchCompression(getCompression(c.Compression)),
	}

	if c.BatchSize > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(c.BatchSize))
	}

	if c.WriteTimeout != 0 {
		opts = append(opts, kgo.ProduceRequestTimeout(c.WriteTimeout))
	}

	if c.RetryTimeout != 0 {
		opts = append(opts, kgo.ProduceRequestTimeout(c.RetryTimeout))
	}

	balancer := getGroupBalancer(s.config.Balance)

	if balancer != nil {
		opts = append(opts, kgo.Balancers(balancer))
	}

	if c.SASL.Enabled != nil && *c.SASL.Enabled == true {
		mch := getMechanism(c.SASL)
		if mch != nil {
			opts = append(opts, kgo.SASL(mch))
		}
	}

	if c.TLS.Enabled != nil && *c.TLS.Enabled == true {
		var tlsCfg *tls.Config
		var err error
		if tlsCfg, err = NewTLSConfig(c.TLS.CaCertFiles, c.TLS.ClientCertFile, c.TLS.ClientKeyFile, c.TLS.EndpIdentAlgo == ""); err != nil {
			return err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	cl, err := kgo.NewClient(opts...)

	if err != nil {
		log.Error("kgo.NewClient error:%s", err)
		return err
	}

	s.writer = cl
	return nil
}

func (s *Sink) Stop() {
	if s.writer != nil {
		s.writer.Close()
	}
}

func (s *Sink) Consume(batch api.Batch) api.Result {
	events := batch.Events()
	l := len(events)
	if l == 0 {
		return nil
	}

	records := make([]*kgo.Record, 0, l)

	for _, e := range events {
		topic, err := s.selectTopic(e)
		if err != nil {
			log.Error("select kafka topic error: %+v", err)
			return result.Fail(err)
		}

		msg, err := s.cod.Encode(e)
		if err != nil {
			log.Warn("encode event error: %+v", err)
			return result.Fail(err)
		}

		records = append(records, &kgo.Record{
			Value: msg,
			Topic: topic,
		})
	}

	ctx := context.Background()

	if s.writer != nil {
		ret := s.writer.ProduceSync(ctx, records...)
		if ret.FirstErr() != nil {
			return result.Fail(errors.New(fmt.Sprintf("franz ProduceSync error:%s", ret.FirstErr())))
		}
		return result.Success()
	}

	return result.Fail(errors.New("kafka sink writer not initialized"))
}

func (s *Sink) selectTopic(e api.Event) (string, error) {
	return s.topicPattern.WithObject(runtime.NewObject(e.Header())).Render()
}
