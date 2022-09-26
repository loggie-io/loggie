package localserver

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/result"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/codec"
	"math/rand"
	"time"
)

const Type = "localserver"

func init() {
	pipeline.Register(api.SINK, Type, makeSink)
}

func makeSink(info pipeline.Info) api.Component {
	return NewSink()
}

type Sink struct {
	name   string
	config *Config
	codec  codec.Codec
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

func (s *Sink) SetCodec(c codec.Codec) {
	s.codec = c
}

func (s *Sink) String() string {
	return fmt.Sprintf("%s/%s", api.SINK, Type)
}

func (s *Sink) Init(context api.Context) error {
	s.name = context.Name()
	return nil
}

func (s *Sink) Start() error {
	log.Info("%s start", s.String())
	return nil
}

func (s *Sink) Stop() {
}

func (s *Sink) Consume(batch api.Batch, pool api.FlowDataPool) api.Result {
	events := batch.Events()
	l := len(events)
	if l == 0 {
		return nil
	}

	t1 := time.Now()
	for _, e := range events {
		// json encode
		_, err := s.codec.Encode(e)
		if err != nil {
			log.Warn("codec event error: %+v", err)
			continue
		}
		//log.Info("event: %s", string(out))
	}

	time.Sleep(time.Second * time.Duration(rand.Intn(10)))
	t2 := time.Now()
	microseconds := t2.Sub(t1).Microseconds()
	pool.EnqueueRTT(microseconds)

	return result.NewResult(api.SUCCESS)
}
