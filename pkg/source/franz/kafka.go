/*
Copyright 2023 Loggie Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package franz

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/sink/franz"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync"
	"time"
)

const (
	Type = "franzKafka"

	fKafka       = "kafka"
	fOffset      = "offset"
	fPartition   = "partition"
	fTimestamp   = "timestamp"
	fTopic       = "topic"
	fLeaderEpoch = "leaderEpoch"
)

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	return &Source{
		done:      make(chan struct{}),
		config:    &Config{},
		eventPool: info.EventPool,
	}
}

type Source struct {
	name      string
	done      chan struct{}
	closeOnce sync.Once
	config    *Config
	client    *kgo.Client
	eventPool *event.Pool
}

func (k *Source) Config() interface{} {
	return k.config
}

func (k *Source) Category() api.Category {
	return api.SOURCE
}

func (k *Source) Type() api.Type {
	return Type
}

func (k *Source) String() string {
	return fmt.Sprintf("%s/%s", api.SOURCE, Type)
}

func (k *Source) Init(context api.Context) error {
	k.name = context.Name()
	return nil
}

func (k *Source) Start() error {
	c := k.config
	var logger franz.Logger

	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumeRegex(),
		kgo.WithLogger(&logger),
	}

	// set topics
	var confTopic []string
	if k.config.Topic != "" {
		confTopic = append(confTopic, k.config.Topic)
	}
	if len(k.config.Topics) > 0 {
		confTopic = append(confTopic, k.config.Topics...)
	}
	opts = append(opts, kgo.ConsumeTopics(confTopic...))

	// set group id
	if k.config.GroupId != "" {
		opts = append(opts, kgo.ConsumerGroup(k.config.GroupId))
	}

	// set client id
	if k.config.ClientId != "" {
		opts = append(opts, kgo.InstanceID(k.config.ClientId))
	}

	if k.config.FetchMaxWait != 0 {
		opts = append(opts, kgo.FetchMaxWait(k.config.FetchMaxWait))
	}

	if k.config.FetchMaxBytes != 0 {
		opts = append(opts, kgo.FetchMaxBytes(k.config.FetchMaxBytes))
	}

	if k.config.FetchMinBytes != 0 {
		opts = append(opts, kgo.FetchMinBytes(k.config.FetchMinBytes))
	}

	if k.config.FetchMaxPartitionBytes != 0 {
		opts = append(opts, kgo.FetchMaxBytes(k.config.FetchMaxPartitionBytes))
	}

	// set auto commit
	if !k.config.EnableAutoCommit {
		opts = append(opts, kgo.DisableAutoCommit())
	}

	if k.config.AutoCommitInterval != 0 {
		opts = append(opts, kgo.AutoCommitInterval(k.config.AutoCommitInterval))
	}

	if k.config.AutoOffsetReset != "" {
		opts = append(opts, kgo.ConsumeResetOffset(getAutoOffset(c.AutoOffsetReset)))
	}

	if c.SASL.Enabled == true {
		mch := franz.GetMechanism(c.SASL)
		if mch != nil {
			opts = append(opts, kgo.SASL(mch))
		}
	}

	//set tls
	if c.TLS.Enabled == true {
		var tlsCfg *tls.Config
		var err error
		if tlsCfg, err = franz.NewTLSConfig(c.TLS.CaCertFiles, c.TLS.ClientCertFile, c.TLS.ClientKeyFile, c.TLS.EndpIdentAlgo == ""); err != nil {
			return err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	// new client
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Error("kgo.NewClient error:%s", err)
		return err
	}

	k.client = cl
	return nil
}

func (k *Source) Stop() {
	k.closeOnce.Do(func() {
		close(k.done)

		if k.client != nil {
			k.client.Close()
		}
	})
}

func (k *Source) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", k.String())

	if k.client == nil {
		log.Error("kafka consumer not initialized yet")
		return
	}

	wg := sync.WaitGroup{}
	for i := 0; i < k.config.Worker; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-k.done:
					return

				default:
					err := k.consume(productFunc)
					if err != nil {
						log.Error("%+v", err)
					}
				}
			}
		}()
	}

	wg.Wait()
}

func (k *Source) consume(productFunc api.ProductFunc) error {
	fetches := k.client.PollFetches(context.Background())
	if fetches.IsClientClosed() {
		return nil
	}

	if errs := fetches.Errors(); len(errs) > 0 {
		// All errors are retried internally when fetching, but non-retriable errors are
		// returned from polls so that users can notice and take action.
		return errors.Errorf("consumer read message error: %v", errs)
	}

	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()

		e := k.eventPool.Get()
		header := e.Header()
		if k.config.AddonMeta != nil && *k.config.AddonMeta == true {
			if header == nil {
				header = make(map[string]interface{})
			}

			// set default metadata
			header[fKafka] = map[string]interface{}{
				fOffset:    record.Offset,
				fPartition: record.Partition,
				fTimestamp: record.Timestamp.Format(time.RFC3339),
				fTopic:     record.Topic,
			}
			for _, h := range record.Headers {
				header[h.Key] = string(h.Value)
			}
		}

		meta := e.Meta()
		if !k.config.EnableAutoCommit {
			// set fields to metadata for kafka commit message
			if meta == nil {
				meta = event.NewDefaultMeta()
			}
			meta.Set(fOffset, record.Offset)
			meta.Set(fPartition, record.Partition)
			meta.Set(fTopic, record.Topic)
			meta.Set(fLeaderEpoch, record.LeaderEpoch)
		}

		e.Fill(meta, header, record.Value)

		productFunc(e)
	}

	return nil
}

func (k *Source) Commit(events []api.Event) {
	// commit when sink ack
	if !k.config.EnableAutoCommit {
		var records []*kgo.Record
		for _, e := range events {
			meta := e.Meta()

			var mTopic, mPartition, mOffset interface{}
			mTopic, exist := meta.Get(fTopic)
			if !exist {
				continue
			}
			mPartition, exist = meta.Get(fPartition)
			if !exist {
				continue
			}
			mOffset, exist = meta.Get(fOffset)
			if !exist {
				continue
			}
			mLeaderEpoch, exist := meta.Get(fLeaderEpoch)
			if !exist {
				continue
			}

			records = append(records, &kgo.Record{
				Topic:       mTopic.(string),
				Partition:   mPartition.(int32),
				Offset:      mOffset.(int64),
				LeaderEpoch: mLeaderEpoch.(int32),
			})
		}
		if len(records) > 0 {
			err := k.client.CommitRecords(context.Background(), records...)
			if err != nil {
				log.Error("consumer manually commit message error: %v", err)
			}
		}
	}

	k.eventPool.PutAll(events)
}
