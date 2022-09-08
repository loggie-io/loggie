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
	"errors"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/result"
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
	RetryMaxCount               int           `yaml:"retryMaxCount,omitempty" default:"0"`
	CleanDataTimeout            time.Duration `yaml:"cleanDataTimeout" default:"5s"`
}

type retryMeta struct {
	count int
}

type Opt int32

type Interceptor struct {
	stop        atomic.Value
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

func (i *Interceptor) Init(context api.Context) error {
	i.name = context.Name()
	i.stop.Store(false)
	i.done = make(chan struct{})
	i.countDown = &sync.WaitGroup{}
	i.lock = &sync.Mutex{}
	i.cond = sync.NewCond(i.lock)
	i.in = make(chan api.Batch)
	i.pauseSign.Store(false)
	i.signChan = make(chan Opt)
	i.initBackOff()
	return nil
}

func (i *Interceptor) initBackOff() {
	bo := backoff.NewExponentialBackoff()
	// bo.MaxElapsedTime = 0
	i.bo = bo
}

func (i *Interceptor) Start() error {
	go i.run()
	log.Debug("%s start", i.String())
	return nil
}

func (i *Interceptor) Stop() {
	i.stop.Store(true)
	close(i.done)
	i.countDown.Wait()
	i.wakeUp()
	go i.cleanData()
	log.Debug("%s stop", i.String())
}

func (i *Interceptor) Intercept(invoker sink.Invoker, invocation sink.Invocation) api.Result {
	batch := invocation.Batch
	retryBatch := i.isRetryBatch(batch)
	// retry goroutine will not be paused
	if !retryBatch && i.pause() {
		i.wait()
	}
	r := invoker.Invoke(invocation)
	if i.isStop() {
		return r
	}
	if r.Status() != api.SUCCESS {
		rm := i.retryMeta(batch)
		retryMaxCount := i.config.RetryMaxCount
		if rm != nil && retryMaxCount > 0 && retryMaxCount < rm.count {
			return result.DropWith(errors.New(fmt.Sprintf("retry reaches the limit: retryMaxCount(%d)", retryMaxCount)))
		}
		i.in <- batch
	} else {
		if retryBatch {
			// sign backoff reset
			i.signChan <- Reset
		}
	}
	return r
}

func (i *Interceptor) run() {
	var (
		initD  = 500 * time.Millisecond
		buffer = make([]api.Batch, 0)
		active api.Batch
		out    chan<- api.Batch
		c      <-chan time.Time
		t      = time.NewTimer(initD)
	)

	i.countDown.Add(1)
	defer func() {
		i.countDown.Done()
		t.Stop()
		go i.cleanBuffer(buffer)
	}()

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
	log.Debug("consumers will be awake")
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

func (i *Interceptor) isStop() bool {
	return i.stop.Load().(bool)
}

func (i *Interceptor) cleanData() {
	t := time.NewTimer(i.config.CleanDataTimeout)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			return
		case <-i.in:
			// ignore
		case <-i.signChan:
			// ignore
		}
	}
}

func (i *Interceptor) cleanBuffer(buffer []api.Batch) {
	if len(buffer) == 0 {
		return
	}

	t := time.NewTimer(i.config.CleanDataTimeout)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			return
		case i.surviveChan <- buffer[0]:
			buffer = buffer[1:]
			if len(buffer) == 0 {
				return
			}
		}
	}
}
