package franz

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"github.com/loggie-io/loggie/pkg/util/pattern"
	"github.com/twmb/franz-go/pkg/kgo"
)

const Type = "franz"

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

func (s *Sink) Start() error {
	c := s.config
	// One client can both produce and consume!
	// Consuming can either be direct (no consumer group), or through a group. Below, we use a group.
	balancer := getGroupBalancer(s.config.Balance)
	if balancer == nil {
		log.Error("GetGroupBalancer(%s) is nil", s.config.Balance)
		return errors.New(fmt.Sprintf("GetGroupBalancer(%s) is nil", s.config.Balance))
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.Balancers(balancer),
		kgo.MaxBufferedRecords(c.BatchSize),
		kgo.ProducerBatchMaxBytes(c.BatchBytes),
		kgo.ProduceRequestTimeout(c.WriteTimeout),
		kgo.RetryTimeout(c.RetryTimeout),
		kgo.MaxConcurrentFetches(c.MaxConcurrentFetches),
		kgo.FetchMaxBytes(c.FetchMaxBytes), //134 MB
		kgo.BrokerMaxReadBytes(c.BrokerMaxReadBytes),
		kgo.ProducerBatchCompression(getCompression(c.Compression)),
	}

	if c.SASL.Enable != nil && *c.SASL.Enable == true {
		mch := getMechanism(c.SASL)
		if mch != nil {
			opts = append(opts, kgo.SASL(mch))
		}
	}

	if c.TLS.Enable != nil && *c.TLS.Enable == true {
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
	ctx := context.Background()

	record := &kgo.Record{Topic: "foo", Value: []byte("bar")}
	s.writer.ProduceSync(ctx, record, func(_ *kgo.Record, err error) {
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
		}
	})

	return nil
}

func (s *Sink) onPartitionRevoked(_ context.Context, _ *kgo.Client, _ map[string][]int32) {
	//begin := time.Now()
	//k.cleanupFn()
	//util.Logger.Info("consumer group cleanup",
	//	zap.String("task", k.taskCfg.Name),
	//	zap.String("consumer group", k.taskCfg.ConsumerGroup),
	//	zap.Duration("cost", time.Since(begin)))
}
