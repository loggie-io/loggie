package pulsar

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"github.com/pkg/errors"
	"time"
)

const Type = "pulsar"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

// Sink ...
type Sink struct {
	config *pulsarConfig
	clt    *ProducerClient
	cod    codec.Codec
}

type ProducerClient struct {
	pulsarClient    pulsar.Client
	clientOptions   pulsar.ClientOptions
	producerOptions pulsar.ProducerOptions
	producer        pulsar.Producer
}

func NewSink() *Sink {
	return &Sink{
		config: &pulsarConfig{},
	}
}

func (s *Sink) Config() interface{} {
	return s.config
}

func (s *Sink) SetCodec(c codec.Codec) {
	s.cod = c
}

func (s *Sink) Category() api.Category {
	return api.SINK
}

func (s *Sink) Type() api.Type {
	return Type
}

func (s *Sink) Init(_ api.Context) error {
	return nil
}

func (s *Sink) Start() error {
	c := s.config
	log.Debug("init pulsar client config")
	clientOptions, producerOptions, err := getOptions(c)
	if err != nil {
		return err
	}
	client, err := newPulsarClient(*clientOptions, *producerOptions)
	if err != nil {
		return errors.WithMessagef(err, "new pulsar client failed")
	}
	if client == nil {
		return errors.New("client nil")
	}
	client.pulsarClient, err = pulsar.NewClient(client.clientOptions)
	if err != nil {
		log.Debug("Create pulsar producer failed: %v", err)
		return err
	}
	client.producer, err = client.pulsarClient.CreateProducer(client.producerOptions)
	if err != nil {
		log.Debug("Create pulsar producer failed: %v", err)
		return err
	}
	s.clt = client
	return nil
}

func newPulsarClient(
	clientOptions pulsar.ClientOptions,
	producerOptions pulsar.ProducerOptions,
) (*ProducerClient, error) {
	c := &ProducerClient{
		clientOptions:   clientOptions,
		producerOptions: producerOptions,
	}
	return c, nil
}

func (s *Sink) String() string {
	return fmt.Sprintf("%s/%s", api.SINK, Type)
}

func (s *Sink) Stop() {
	if s.clt != nil {
		s.clt.pulsarClient.Close()
	}
}

func (s *Sink) Consume(batch api.Batch) api.Result {
	events := batch.Events()
	var sendErr error
	for i := range events {
		event := &events[i]
		serializerEncode, err := s.cod.Encode(*event)
		if err != nil {
			log.Warn("encode event error: %+v", err)
			return result.Fail(err)
		}
		pTime := time.Now()
		s.clt.producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			EventTime: pTime,
			Key:       fmt.Sprintf("%d", pTime.Nanosecond()),
			Payload:   serializerEncode,
		}, func(msgId pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			if err != nil {
				sendErr = err
				log.Error("send event failed, msgId:%s", msgId)
			} else {
				log.Debug("Pulsar send message event success")
			}
		})
		if sendErr != nil {
			return result.Fail(sendErr)
		}
	}

	return result.Success()
}
