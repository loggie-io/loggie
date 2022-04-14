/*
Copyright 2021 Loggie Authors

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

package retry

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/sink"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/mmaxiaolei/backoff"
)

const Type = "retry"

var (
	retryTag = "loggie-system-retry"

	Reset = Opt(0)
)

func init() {
	pipeline.Register(api.INTERCEPTOR, Type, makeInterceptor)
}

func makeInterceptor(info pipeline.Info) api.Component {
	return &Interceptor{
		config:      &Config{},
		surviveChan: info.SurviveChan,
	}
}

type Config struct {
	interceptor.ExtensionConfig `yaml:",inline"`
	RetryMaxCount               int `yaml:"retryMaxCount,omitempty" default:"0"`
}

type retryMeta struct {
	count int
}

type Opt int32

type Interceptor struct {
	done        chan struct{}
	name        string
	config      *Config
	lock        *sync.Mutex
	cond        *sync.Cond
	countDown   *sync.WaitGroup
	surviveChan chan<- api.Batch
	in          chan api.Batch
	pauseSign   atomic.Value
	bo          backoff.Backoff
	signChan    chan Opt
}

func (i *Interceptor) Config() interface{} {
	return i.config
}

func (i *Interceptor) Category() api.Category {
	return api.INTERCEPTOR
}

func (i *Interceptor) Type() api.Type {
	return Type
}

func (i *Interceptor) String() string {
	return fmt.Sprintf("%s/%s", i.Category(), i.Type())
}

func (i *Interceptor) Init(context api.Context) {
	i.name = context.Name()
	i.done = make(chan struct{})
	i.countDown = &sync.WaitGroup{}
	i.lock = &sync.Mutex{}
	i.cond = sync.NewCond(i.lock)
	i.in = make(chan api.Batch)
	i.pauseSign.Store(false)
	i.signChan = make(chan Opt)
	i.initBackOff()
}

func (i *Interceptor) initBackOff() {
	bo := backoff.NewExponentialBackoff()
	//bo.MaxElapsedTime = 0
	i.bo = bo
}

func (i *Interceptor) Start() {
	go i.run()
	log.Info("%s start", i.String())
}

func (i *Interceptor) Stop() {
	close(i.done)
	i.countDown.Wait()
	i.wakeUp()
	log.Info("%s stop", i.String())
}

func (i *Interceptor) Intercept(invoker sink.Invoker, invocation sink.Invocation) api.Result {
	batch := invocation.Batch
	retryBatch := i.isRetryBatch(batch)
	// retry goroutine will not be pause
	if !retryBatch && i.pause() {
		i.wait()
	}
	result := invoker.Invoke(invocation)
	if result.Status() != api.SUCCESS {
		rm := i.retryMeta(batch)
		retryMaxCount := i.config.RetryMaxCount
		if rm != nil && retryMaxCount > 0 && retryMaxCount < rm.count {
			result.ChangeStatusTo(api.DROP)
			return result
		}
		i.in <- batch
	} else {
		if retryBatch {
			// sign backoff reset
			i.signChan <- Reset
		}
	}
	return result
}

func (i *Interceptor) run() {
	i.countDown.Add(1)
	defer i.countDown.Done()

	var (
		initD  = 500 * time.Millisecond
		buffer = make([]api.Batch, 0)
		active api.Batch
		out    chan<- api.Batch
		c      <-chan time.Time
		t      = time.NewTimer(initD)
	)
	defer t.Stop()
	for {
		select {
		case <-i.done:
			return
		case opt := <-i.signChan:
			if opt == Reset {
				i.bo.Reset()
				i.wakeUp()
			}
		case b := <-i.in:
			buffer = append(buffer, b)

			// block consumer
			l := len(buffer)
			if l > 1 {
				log.Warn("%s retry buffer size(%d) too large", i.String(), l)
				i.pauseSign.Store(true)
			}
		case <-c:
			active = buffer[0]
			meta := active.Meta()
			var rm *retryMeta
			if value, ok := meta[retryTag]; ok {
				rm = value.(*retryMeta)
				rm.count++
			} else {
				rm = &retryMeta{
					count: 1,
				}
				meta[retryTag] = rm
			}

			out = i.surviveChan
			c = nil
		case out <- active:
			buffer = buffer[1:]
			if len(buffer) > 0 {
				duration := i.bo.Next()
				log.Info("next retry duration: %dms", duration/time.Millisecond)
				t.Reset(duration)
			} else {
				t.Reset(initD)
			}
			// reset
			active = nil
			out = nil
		}

		if len(buffer) > 0 && active == nil {
			c = t.C
		}
	}
}

func (i *Interceptor) pause() bool {
	return i.pauseSign.Load().(bool)
}

func (i Interceptor) isRetryBatch(batch api.Batch) bool {
	rm := i.retryMeta(batch)
	return rm != nil && rm.count > 0
}

func (i *Interceptor) wait() {
	log.Info("consumer will be pause")
	i.lock.Lock()
	i.cond.Wait()
	i.lock.Unlock()
}

func (i *Interceptor) wakeUp() {
	i.pauseSign.Store(false)
	i.signAll()
}

func (i *Interceptor) signAll() {
	log.Info("consumers will be awake")
	i.lock.Lock()
	i.cond.Broadcast()
	i.lock.Unlock()
}

func (i *Interceptor) retryMeta(batch api.Batch) *retryMeta {
	meta := batch.Meta()
	if value, ok := meta[retryTag]; ok {
		return value.(*retryMeta)
	}
	return nil
}

func (i *Interceptor) Order() int {
	return i.config.Order
}

func (i *Interceptor) BelongTo() (componentTypes []string) {
	return i.config.BelongTo
}

func (i *Interceptor) IgnoreRetry() bool {
	return false
}
