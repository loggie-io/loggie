package franz

import (
	"context"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

func Test_SinkerWrite(t *testing.T) {
	log.InitDefaultLogger()
	var c Config
	c.Brokers = []string{
		"192.168.110.8:9092",
		"192.168.110.12:9092",
		"192.168.110.16:9092",
	}

	sinker := NewSink()
	sinker.config = &c
	var sinkerContext api.Context
	sinker.Init(sinkerContext)
	sinker.Start()
	ctx := context.Background()
	record := &kgo.Record{Topic: "franz-test", Value: []byte("1111111111111111111111111")}
	ret := sinker.writer.ProduceSync(ctx, record)
	fmt.Println(ret)
}
