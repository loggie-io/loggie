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

package pipeline

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/concurrency"
	"github.com/loggie-io/loggie/pkg/core/context"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/flowdatapool"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/queue"
	"github.com/loggie-io/loggie/pkg/core/sink"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/eventbus"
	sinkcodec "github.com/loggie-io/loggie/pkg/sink/codec"
	sourcecodec "github.com/loggie-io/loggie/pkg/source/codec"
	"github.com/loggie-io/loggie/pkg/util"
)

const (
	FieldsUnderRoot = event.PrivateKeyPrefix + "FieldsUnderRoot"
	FieldsUnderKey  = event.PrivateKeyPrefix + "FieldsUnderKey"

	fieldsFromPathMaxBytes = 1024
)

var (
	ErrPipelineNameRequired   = errors.New("pipelines[n].name is required")
	ErrSourceNameRequired     = errors.New("sources[n].name is required")
	ErrPipelineSourceRequired = errors.New("pipelines[n].source is required")
	ErrPipelineSinkRequired   = errors.New("pipelines[n].sink is required")
)

type Pipeline struct {
	name          string
	config        Config
	done          chan struct{}
	info          Info
	r             *RegisterCenter
	ns            map[string]api.Source // key:name|value:source
	nq            map[string]api.Queue  // key:name|value:queue
	flowPool      api.FlowDataPool
	flowPoolDone  chan struct{}
	gpool         *ants.Pool
	gpoolMaxSize  int
	outChans      []chan api.Batch
	countDown     sync.WaitGroup
	retryOutFuncs []api.OutFunc
	index         uint32
	epoch         *Epoch
	envMap        map[string]interface{}
	pathMap       map[string]interface{}
	outfunc       api.OutFunc
	retryoutfunc  api.OutFunc
	sinkinfo      sink.Info
	concurrency   concurrency.Config

	Running bool
}

func NewPipeline(pipelineConfig *Config) *Pipeline {
	registerCenter := NewRegisterCenter()
	return &Pipeline{
		config: *pipelineConfig,
		done:   make(chan struct{}),
		info: Info{
			Stop:        false,
			R:           registerCenter,
			SurviveChan: make(chan api.Batch, pipelineConfig.Sink.Parallelism+1),
		},
		r:           registerCenter,
		concurrency: pipelineConfig.Sink.Concurrency,
	}
}

func (p *Pipeline) Stop() {
	if p.info.Stop == true {
		return
	}

	p.info.Stop = true
	done := make(chan struct{})
	// clean out chan: in case blocking
	p.cleanOutChan(done)
	// 0. stop sink consumer
	p.stopSinkConsumer()
	// 1. stop source product
	p.stopSourceProduct()
	// 2. stop queue
	p.stopQueue()
	// 3. stop component
	// care about stopped components
	p.stopComponents()
	// 4. stop listeners
	p.stopListeners()
	// 5. clean data(out chan、registry center、and so on)
	p.cleanData()
	close(done)

	log.Info("stop pipeline with epoch: %+v", p.epoch)
}

func (p *Pipeline) stopSinkConsumer() {
	// stop sink interceptor
	for c, inter := range p.r.LoadCodeInterceptors() {
		if i, ok := inter.(sink.Interceptor); ok {
			log.Info("stop sink interceptor: %s", i.String())
			i.Stop()
			p.r.RemoveByCode(c)
			log.Info("sink interceptor stopped: %s", i.String())
		}
	}
	// stop sink consumer and survive
	close(p.done)
	p.stopGPool()
	p.countDown.Wait()
}

func (p *Pipeline) stopSourceProduct() {
	taskName := fmt.Sprintf("stop sources of pipeline(%s)", p.name)
	namedJob := make(map[string]func())
	sources := p.r.LoadCodeCategoryComponents(api.SOURCE)
	for code, s := range sources {
		localCode := code
		localSource := s

		p.r.RemoveByCode(localCode)
		jobName := fmt.Sprintf("stop source(%s)", localCode)
		job := func() {
			localSource.Stop()
			p.reportMetricWithCode(localCode, localSource, eventbus.ComponentStop)
		}
		namedJob[jobName] = job
	}
	util.AsyncRunGroup(taskName, namedJob)
}

func (p *Pipeline) stopQueue() {
	queues := p.r.LoadCodeCategoryComponents(api.QUEUE)
	for c, q := range queues {
		localCode := c
		localQueue := q

		p.r.RemoveByCode(localCode)
		localQueue.Stop()
		p.reportMetricWithCode(localCode, localQueue, eventbus.ComponentStop)
	}
}

func (p *Pipeline) stopComponents() {
	log.Debug("stopping components of pipeline %s", p.name)
	components := p.r.LoadCodeComponents()
	for c, v := range components {
		// async stop with timeout
		localCode := c
		localComponent := v

		p.r.RemoveByCode(localCode)
		util.AsyncRunWithTimeout(func() {
			localComponent.Stop()
			p.reportMetricWithCode(localCode, localComponent, eventbus.ComponentStop)
		}, p.config.CleanDataTimeout)
	}
}

func (p *Pipeline) stopListeners() {
	for _, listener := range p.r.nameListeners {
		// async stop with timeout
		l := listener
		util.AsyncRunWithTimeout(func() {
			l.Stop()
		}, p.config.CleanDataTimeout)
	}
}

func (p *Pipeline) cleanData() {
	// clean registry center
	p.r.cleanData()
	// clean pipeline
	p.ns = nil
	p.nq = nil
	p.outChans = nil
	p.r = nil
}

func (p *Pipeline) cleanOutChan(done <-chan struct{}) {
	for _, outChan := range p.outChans {
		out := outChan
		go p.consumerOutChanAndDrop(out, done)
	}
}

func (p *Pipeline) consumerOutChanAndDrop(out chan api.Batch, done <-chan struct{}) {
	dropAndRelease := func(batch api.Batch) {
		if batch != nil {
			events := batch.Events()
			if events != nil {
				p.info.EventPool.PutAll(events)
			}
			batch.Release()
		}
	}
	for {
		select {
		case <-done:
			return
		case b := <-out:
			dropAndRelease(b)
		case b := <-p.info.SurviveChan:
			dropAndRelease(b)
		}
	}
}

func (p *Pipeline) Start() error {
	pipelineConfig := p.config
	p.init(pipelineConfig)

	// 1. start interceptor
	if err := p.startInterceptor(pipelineConfig.Interceptors); err != nil {
		return err
	}
	// 2. start sink
	if err := p.startSink(pipelineConfig.Sink); err != nil {
		return err
	}
	// 3. start source
	if err := p.startSource(pipelineConfig.Sources); err != nil {
		return err
	}
	// 4. start queue
	if err := p.startQueue(*pipelineConfig.Queue); err != nil {
		return err
	}
	// 5. start sink consumer
	p.startSinkConsumer(pipelineConfig.Sink)
	// 6. start source product
	p.startSourceProduct(pipelineConfig.Sources)

	go p.survive()
	log.Info("pipeline start with epoch: %+v", p.epoch)
	return nil
}

func (p *Pipeline) init(pipelineConfig Config) {
	p.name = pipelineConfig.Name
	p.config = pipelineConfig
	if p.epoch.IsEmpty() {
		p.epoch = NewEpoch(p.name)
	}
	p.epoch.Increase()
	registerCenter := NewRegisterCenter()
	p.r = registerCenter
	p.info.R = registerCenter
	p.info.PipelineName = p.name
	p.info.Epoch = p.epoch
	p.info.SinkCount = pipelineConfig.Sink.Parallelism
	p.outChans = make([]chan api.Batch, 0)
	p.done = make(chan struct{})
	p.info.Stop = false
	p.ns = make(map[string]api.Source)
	p.nq = make(map[string]api.Queue)
	p.envMap = make(map[string]interface{})
	p.pathMap = make(map[string]interface{})

	// init event pool
	p.info.EventPool = event.NewDefaultPool(pipelineConfig.Queue.BatchSize * (p.info.SinkCount + 1))
}

func (p *Pipeline) startInterceptor(interceptorConfigs []*interceptor.Config) error {
	for _, iConfig := range interceptorConfigs {
		if iConfig.Enabled != nil && *iConfig.Enabled == false {
			log.Info("interceptor %s is disabled", iConfig.Type)
			continue
		}
		ctx := context.NewContext(iConfig.Name, api.Type(iConfig.Type), api.INTERCEPTOR, iConfig.Properties)
		err := p.startComponent(ctx)
		if err != nil {
			return errors.WithMessage(err, "start interceptor failed")
		}
	}
	return nil
}

func (p *Pipeline) startQueue(queueConfig queue.Config) error {
	ctx := context.NewContext(queueConfig.Name, api.Type(queueConfig.Type), api.QUEUE, queueConfig.Properties)
	err := p.startComponent(ctx)
	if err != nil {
		return errors.WithMessage(err, "start queue failed")
	}
	q := p.r.LoadQueue(api.Type(queueConfig.Type), queueConfig.Name)
	p.nq[queueConfig.Name] = q
	p.outChans = append(p.outChans, q.OutChan())
	return nil
}

func (p *Pipeline) startComponent(ctx api.Context) error {
	component, _ := GetWithType(ctx.Category(), ctx.Type(), p.info)
	if err := p.startWithComponent(component, ctx); err != nil {
		// log.Error("start component failed: %v", err)
		return err
	}
	return nil
}

func (p *Pipeline) startWithComponent(component api.Component, ctx api.Context) error {
	// unpack config from properties
	err := cfg.UnpackFromCommonCfg(ctx.Properties(), component.Config()).Defaults().Do()
	if err != nil {
		return errors.WithMessagef(err, "unpack component %s/%s", component.Category(), component.Type())
	}

	err = component.Init(ctx)
	if err != nil {
		return errors.WithMessagef(err, "init component %s/%s", component.Category(), component.Type())
	}

	err = component.Start()
	if err != nil {
		return errors.WithMessagef(err, "start component %s/%s", component.Category(), component.Type())
	}

	err = p.r.Register(component, ctx.Name())
	if err != nil {
		return err
	}
	p.reportMetric(ctx.Name(), component, eventbus.ComponentStart)
	return nil
}

func (p *Pipeline) afterSinkConsumer(b api.Batch, result api.Result) {
	// commit to source and release batch
	// we use the if/else instead of switch/case cause of performance in golang
	status := result.Status()
	if status == api.SUCCESS {
		p.finalizeBatch(b)
		return
	}

	if status == api.FAIL {
		log.Error("sink sends batch failed: %s", result.Error())
		return
	}

	if status == api.DROP {
		if result.Error() != nil {
			log.Error("drop batch due to: %s", result.Error())
		}
		p.finalizeBatch(b)
		return
	}
}

// commit to source and release batch
func (p *Pipeline) finalizeBatch(batch api.Batch) {

	nes := make(map[string][]api.Event)
	events := batch.Events()
	l := len(events)
	for _, e := range events {
		sourceName := e.Meta().Source()
		es, ok := nes[sourceName]
		if !ok {
			es = make([]api.Event, 0, l)
		}
		es = append(es, e)
		nes[sourceName] = es
	}
	for sn, es := range nes {
		p.ns[sn].Commit(es)
	}

	batch.Release()
}

func (p *Pipeline) validateComponent(ctx api.Context) error {
	component, err := GetWithType(ctx.Category(), ctx.Type(), p.info)
	if err != nil {
		return err
	}
	return cfg.UnpackFromCommonCfg(ctx.Properties(), component.Config()).Defaults().Validate().Do()
}

func (p *Pipeline) validate() error {
	pipelineConfig := &p.config
	if pipelineConfig.Name == "" {
		return ErrPipelineNameRequired
	}

	for _, iConfig := range pipelineConfig.Interceptors {
		ctx := context.NewContext(iConfig.Name, api.Type(iConfig.Type), api.INTERCEPTOR, iConfig.Properties)
		if err := p.validateComponent(ctx); err != nil {
			return err
		}
	}

	qConfig := pipelineConfig.Queue
	ctx := context.NewContext(qConfig.Name, api.Type(qConfig.Type), api.QUEUE, qConfig.Properties)
	if err := p.validateComponent(ctx); err != nil {
		return err
	}

	sinkConfig := pipelineConfig.Sink
	if sinkConfig == nil || sinkConfig.Type == "" {
		return ErrPipelineSinkRequired
	}
	ctx = context.NewContext(sinkConfig.Name, api.Type(sinkConfig.Type), api.SINK, sinkConfig.Properties)
	if err := p.validateComponent(ctx); err != nil {
		return err
	}

	unique := make(map[string]struct{})
	if len(pipelineConfig.Sources) == 0 {
		return ErrPipelineSourceRequired
	}
	for _, sourceConfig := range pipelineConfig.Sources {
		if sourceConfig.Name == "" {
			return ErrSourceNameRequired
		}
		if _, ok := unique[sourceConfig.Name]; ok {
			return errors.Errorf("source name %s is duplicated", sourceConfig.Name)
		}
		unique[sourceConfig.Name] = struct{}{}
		ctx := context.NewContext(sourceConfig.Name, api.Type(sourceConfig.Type), api.SOURCE, sourceConfig.Properties)
		if err := p.validateComponent(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) startSink(sinkConfigs *sink.Config) error {
	p.retryOutFuncs = make([]api.OutFunc, 0)
	ctx := context.NewContext(sinkConfigs.Name, api.Type(sinkConfigs.Type), api.SINK, sinkConfigs.Properties)

	// get codec config
	codecConf := sinkConfigs.Codec

	// init codec
	cod, ok := sinkcodec.Get(codecConf.Type)
	if !ok {
		return errors.Errorf("codec %s cannot be found", codecConf.Type)
	}
	if conf, ok := cod.(api.Config); ok {
		err := cfg.UnpackFromCommonCfg(codecConf.CommonCfg, conf.Config()).Defaults().Do()
		if err != nil {
			// since Loggie has validate the configuration before start, we would never reach here
			return errors.WithMessage(err, "unpack codec config error")
		}
	}
	cod.Init(&codecConf)

	// set codec to sink
	component, _ := GetWithType(ctx.Category(), ctx.Type(), p.info)
	if si, ok := component.(sinkcodec.SinkCodec); ok {
		si.SetCodec(cod)
	}

	return p.startWithComponent(component, ctx)
}

func (p *Pipeline) startSinkConsumer(sinkConfig *sink.Config) {
	interceptors := make([]sink.Interceptor, 0)
	for _, inter := range p.r.LoadInterceptors() {
		i, ok := inter.(sink.Interceptor)
		if !ok {
			continue
		}
		interceptors = append(interceptors, i)
	}

	si := sink.Info{
		Sink:         p.r.LoadSink(api.Type(sinkConfig.Type), sinkConfig.Name),
		Queue:        p.r.LoadDefaultQueue(),
		Interceptors: interceptors,
	}
	p.sinkinfo = si
	// combine component default interceptors
	interceptors = append(interceptors, collectComponentDependencySinkInterceptors(si.Sink)...)
	interceptors = append(interceptors, collectComponentDependencySinkInterceptors(si.Queue)...)

	p.flowPool = flowdatapool.InitDataPool(100)
	p.flowPool.SetEnabled(p.concurrency.Enable)
	invoker := &sink.SubscribeInvoker{}
	sinkInvokerChain := buildSinkInvokerChain(invoker, interceptors, false)
	retrySinkInvokerChain := buildSinkInvokerChain(invoker, interceptors, true)
	outFunc := func(batch api.Batch) api.Result {
		result := sinkInvokerChain.Invoke(sink.Invocation{
			Batch:    batch,
			Sink:     si.Sink,
			FlowPool: p.flowPool,
		})
		return result
	}
	p.outfunc = outFunc
	retryOutFunc := func(batch api.Batch) api.Result {
		result := retrySinkInvokerChain.Invoke(sink.Invocation{
			Batch:    batch,
			Sink:     si.Sink,
			FlowPool: p.flowPool,
		})
		return result
	}
	p.retryoutfunc = retryOutFunc
	for i := 0; i < sinkConfig.Parallelism; i++ {
		p.retryOutFuncs = append(p.retryOutFuncs, retryOutFunc)
	}

	gpool, _ := ants.NewPool(10)
	p.gpool = gpool
	p.flowPoolDone = make(chan struct{})

	concurrencyEnabled := p.concurrency.Enable
	if concurrencyEnabled {
		p.gpoolMaxSize = p.concurrency.Goroutine.MaxGoroutine
		p.tuneGPool(2)
		p.startGPoolCalculator()
	} else {
		p.gpoolMaxSize = sinkConfig.Parallelism
		p.tuneGPool(sinkConfig.Parallelism)
	}

}

// outfunc may have been combined, but batch has been released in advance
func (p *Pipeline) sinkInvokeLoop(info sink.Info, outFunc api.OutFunc) {
	p.countDown.Add(1)
	s := info.Sink
	log.Info("pipeline sink(%s) invoke loop start", s.String())
	defer func() {
		p.countDown.Done()
		log.Info("pipeline sink(%s) invoke loop stop", s.String())
	}()
	q := info.Queue
	outChan := q.OutChan()
	for {
		select {
		case <-p.done:
			return
		case b := <-outChan:
			result := outFunc(b)
			p.afterSinkConsumer(b, result)
		case <-p.flowPoolDone:
			return
		}
	}
}

type checkRttFunc func(new, old float64) bool

func (p *Pipeline) startGPoolCalculator() {
	failedChannel := p.flowPool.GetFailedChannel()
	var rttD float64
	var rttT float64

	stable := false
	targets := make([]int, 0)
	var averageTarget int

	config := p.concurrency
	threshold := config.Goroutine.InitThreshold
	unstableTolerate := config.Goroutine.UnstableTolerate
	channelLenOfCap := config.Goroutine.ChannelLenOfCap

	var check checkRttFunc

	blockJudgeThreshold := config.Rtt.BlockJudgeThreshold
	if strings.Contains(blockJudgeThreshold, "%") {
		float, err := strconv.ParseFloat(blockJudgeThreshold[:len(blockJudgeThreshold)-1], 64)
		if err != nil {
			log.Warn("fail to get blockJudgeThreshold, use default: 120%%")
			float = 120
		}
		log.Debug("blockJudgeThreshold: %f%%", float)
		check = func(v1, v2 float64) bool {
			log.Debug("v1: %f, v2: %f, blockJudgeThreshold: %f%%", v1, v2, float)
			return v1/v2 > float/100
		}
	} else {
		float, err := strconv.ParseFloat(blockJudgeThreshold, 64)
		if err != nil {
			log.Warn("fail to get blockJudgeThreshold, use default: 2.0 s")
			float = 2
		}
		log.Debug("blockJudgeThreshold %f s", float)
		float = float * 1000000
		check = func(v1, v2 float64) bool {
			log.Debug("v1: %f, v2: %f, blockJudgeThreshold: %f", v1, v2, float)
			return (v1 - v2) > float
		}
	}

	newRttWeigh := config.Rtt.NewRttWeigh

	multiIncreaseRatio := config.Ratio.Multi
	linearIncreaseRatio := config.Ratio.Linear
	linearIncreaseRatioSourceBlocked := config.Ratio.LinearWhenBlocked

	unstableDuration := config.Duration.Unstable
	stableDuration := config.Duration.Stable

	tickDuration := unstableDuration

	decreaseReq := make([]bool, 0)
	increaseReq := make([]bool, 0)

	q := p.sinkinfo.Queue.OutChan()

	go func() {
		timer := time.NewTicker(time.Second * time.Duration(tickDuration))
		defer timer.Stop()
		for {
			select {
			case <-p.done:
				return
			case <-failedChannel:

				if !stable {
					if len(targets) == 15 {
						var sum int
						for i := 5; i < 10; i++ {
							sum += targets[i]
						}
						averageTarget = sum / 10
						stable = true
						tickDuration = stableDuration
						timer.Reset(time.Second * time.Duration(tickDuration))
						log.Info("now stable mode, average target %d", averageTarget)
					}
				}

				log.Info("failed response received")
				allRtt := p.flowPool.DequeueAllRtt()
				count := len(allRtt)
				log.Debug("rtt count %d", count)

				if !stable {
					target := p.gpool.Running() - linearIncreaseRatio
					threshold = target
					targets = append(targets, target)
					p.tuneGPool(target)
				} else {
					decreaseReq = append(decreaseReq, true)
					increaseReq = make([]bool, 0)
					if len(decreaseReq) >= unstableTolerate {
						target := p.gpool.Running() - linearIncreaseRatio
						if target <= averageTarget-linearIncreaseRatio {
							averageTarget = averageTarget - linearIncreaseRatio
							target = averageTarget
						}

						if target < p.gpool.Running() {
							threshold = target
							p.tuneGPool(target)
						}

					} else {
						log.Info("stable mode decrease req, ignored %d", len(decreaseReq))
					}
				}

				if count > 0 {
					var sum int64
					for i := 0; i < count; i++ {
						sum += allRtt[i]
					}
					rttD = float64(sum) / float64(count)
					if rttT != 0 {
						oldRttT := rttT
						rttT = newRttWeigh*rttD + (1-newRttWeigh)*rttT
						log.Debug("old rttT: %f, rttD: %f, new rttT: %f", oldRttT, rttD, rttT)
					} else {
						rttT = rttD
					}
				}

			case <-timer.C:
				if !stable {
					if len(targets) == 15 {
						var sum int
						for i := 5; i < 15; i++ {
							sum += targets[i]
						}
						averageTarget = sum / 10
						stable = true
						tickDuration = stableDuration
						timer.Reset(time.Second * time.Duration(tickDuration))
						log.Info("now stable mode, average target %d", averageTarget)
					}
				}

				log.Info("regular gpool resize")
				allRtt := p.flowPool.DequeueAllRtt()
				count := len(allRtt)
				log.Debug("rtt count %d", count)
				if count > 0 {
					var sum int64
					for i := 0; i < count; i++ {
						sum += allRtt[i]
					}
					rttD = float64(sum) / float64(count)
					if rttT != 0 {
						if check(rttD, rttT) {
							log.Info("rtt too long, decrease gpool size ,rrtD %f,rttT %f", rttD, rttT)
							if !stable {
								target := p.gpool.Running() - linearIncreaseRatio
								threshold = target
								targets = append(targets, target)
								p.tuneGPool(threshold)
							} else {
								decreaseReq = append(decreaseReq, true)
								increaseReq = make([]bool, 0)
								if len(decreaseReq) >= unstableTolerate {
									target := p.gpool.Running() - linearIncreaseRatio
									if target <= averageTarget-linearIncreaseRatio {
										averageTarget = averageTarget - linearIncreaseRatio
										target = averageTarget
									}
									if target < p.gpool.Running() {
										threshold = target
										p.tuneGPool(target)
									}
								} else {
									log.Info("stable mode decrease req, ignored %d", len(decreaseReq))
								}
							}

							oldRttT := rttT
							rttT = newRttWeigh*rttD + (1-newRttWeigh)*rttT
							log.Debug("old rttT: %f, rttD: %f, new rttT: %f", oldRttT, rttD, rttT)
							continue
						} else {
							oldRttT := rttT
							rttT = newRttWeigh*rttD + (1-newRttWeigh)*rttT
							log.Debug("old rttT: %f, rttD: %f, new rttT: %f", oldRttT, rttD, rttT)
						}
					} else {
						rttT = rttD
					}

					pool := p.gpool
					currentPoolCap := pool.Running()
					var targetCap int
					if currentPoolCap < threshold {
						targetCap = currentPoolCap * multiIncreaseRatio
					} else {
						currentChannelLen := len(q)
						currentChannelCap := cap(q)
						if q == nil {
							log.Warn("channel is nil")
						}
						log.Debug("currentChannelLen: %d, currentChannelCap %d", currentChannelLen, currentChannelCap)
						if currentChannelLen == currentChannelCap {
							targetCap = currentPoolCap + linearIncreaseRatioSourceBlocked
						} else if float64(currentChannelLen) > float64(currentChannelCap)*channelLenOfCap {
							targetCap = currentPoolCap + linearIncreaseRatio
						} else {
							targetCap = currentPoolCap
						}

						if !stable {
							log.Info("target append %d", targetCap)
							targets = append(targets, targetCap)
						}

					}

					if targetCap > currentPoolCap {
						if !stable {
							p.tuneGPool(targetCap)
						} else {
							decreaseReq = make([]bool, 0)
							increaseReq = append(increaseReq, true)
							if len(increaseReq) >= unstableTolerate {
								if targetCap >= averageTarget+linearIncreaseRatio {
									averageTarget = averageTarget + linearIncreaseRatio
									targetCap = averageTarget
								}
								threshold = targetCap
								p.tuneGPool(targetCap)
							} else {
								log.Info("stable mode increase req, ignored %d", len(increaseReq))
							}
						}
					}
				}

			}
			sources := p.config.Sources
			for _, source := range sources {
				sinkMetricData := eventbus.SinkMetricData{
					BaseMetric: eventbus.BaseMetric{
						PipelineName: p.name,
						SourceName:   source.Name,
					},
					GoroutinePoolSize: p.gpool.Cap(),
				}
				eventbus.PublishOrDrop(eventbus.SinkMetricTopic, sinkMetricData)
			}
		}
	}()

}

func (p *Pipeline) tuneGPool(targetCap int) {

	if targetCap <= 1 {
		log.Debug("gpool size should be larger than 1")
		targetCap = 2
	}

	if targetCap > p.gpoolMaxSize {
		log.Debug("gpool size should not be larger than %d", p.gpoolMaxSize)
		targetCap = p.gpoolMaxSize
	}

	pool := p.gpool
	if pool.IsClosed() {
		pool.Reboot()
	}
	currentPoolRunning := pool.Running()
	log.Info("currentPoolRunning %d, target %d", currentPoolRunning, targetCap)
	if targetCap > currentPoolRunning {
		pool.Tune(targetCap)
		for i := 0; i < targetCap-currentPoolRunning; i++ {
			err := pool.Submit(func() {
				p.sinkInvokeLoop(p.sinkinfo, p.outfunc)
			})
			if err != nil {
				log.Warn(err.Error())
			}
		}
	} else if targetCap < currentPoolRunning {
		for i := 0; i < currentPoolRunning-targetCap; i++ {
			p.flowPoolDone <- struct{}{}
		}
		pool.Tune(targetCap)
	}
}

func (p *Pipeline) stopGPool() {
	log.Info("stopping goroutine pool")
	pool := p.gpool
	if pool != nil {
		close(p.flowPoolDone)
		pool.Release()
		log.Info("goroutine pool released")
	}

}

func buildSinkInvokerChain(invoker sink.Invoker, interceptors []sink.Interceptor, retry bool) sink.Invoker {
	l := len(interceptors)
	if l == 0 {
		return invoker
	}
	last := invoker
	var interceptorChainName strings.Builder
	interceptorChainName.WriteString("queue->")
	// sort interceptors
	sortableInterceptor := sink.SortableInterceptor(interceptors)
	sortableInterceptor.Sort()
	// build chain
	for i := 0; i < l; i++ {
		tempInterceptor := sortableInterceptor[l-1-i]
		if retry {
			if extension, ok := tempInterceptor.(interceptor.Extension); ok && extension.IgnoreRetry() {
				continue
			}
		}
		next := last
		last = &sink.AbstractInvoker{
			DoInvoke: func(invocation sink.Invocation) api.Result {
				return tempInterceptor.Intercept(next, invocation)
			},
		}

		interceptorChainName.WriteString(sortableInterceptor[i].String())
		interceptorChainName.WriteString("->")
	}
	interceptorChainName.WriteString("sink")
	if retry {
		log.Info("retry interceptor chain: %s", interceptorChainName.String())
	} else {
		log.Info("sink interceptor chain: %s", interceptorChainName.String())
	}
	return last
}

func (p *Pipeline) startSource(sourceConfigs []*source.Config) error {
	for _, sourceConfig := range sourceConfigs {
		if sourceConfig.Enabled != nil && *sourceConfig.Enabled == false {
			log.Info("source %s/%s is disabled", sourceConfig.Type, sourceConfig.Name)
			continue
		}

		ctx := context.NewContext(sourceConfig.Name, api.Type(sourceConfig.Type), api.SOURCE, sourceConfig.Properties)

		component, _ := GetWithType(ctx.Category(), ctx.Type(), p.info)

		// get codec config
		codecConf := sourceConfig.Codec
		if codecConf != nil {
			// init codec
			cod, ok := sourcecodec.Get(codecConf.Type)
			if !ok {
				return errors.Errorf("codec %s cannot be found", codecConf.Type)
			}
			if conf, ok := cod.(api.Config); ok {
				err := cfg.UnpackFromCommonCfg(codecConf.CommonCfg, conf.Config()).Defaults().Do()
				if err != nil {
					// since Loggie has validate the configuration before start, we would never reach here
					return errors.WithMessage(err, "unpack codec config error")
				}
			}
			cod.Init()

			// set codec to source
			if si, ok := component.(sourcecodec.SourceCodec); ok {
				si.SetCodec(cod)
			}
		}

		if sr, ok := component.(source.InjectRawConfig); ok {
			sr.SetSourceConfig(sourceConfig)
		}

		err := p.startWithComponent(component, ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Pipeline) startSourceProduct(sourceConfigs []*source.Config) {
	for _, sc := range sourceConfigs {
		if sc.Enabled != nil && *sc.Enabled == false {
			continue
		}

		sourceConfig := sc
		interceptors := make([]source.Interceptor, 0)
		for _, inter := range p.r.LoadInterceptors() {
			i, ok := inter.(source.Interceptor)
			if !ok {
				continue
			}
			interceptors = append(interceptors, i)
		}

		q := p.r.LoadDefaultQueue()
		si := source.Info{
			Source:       p.r.LoadSource(api.Type(sourceConfig.Type), sourceConfig.Name),
			Queue:        q,
			Interceptors: interceptors,
		}
		p.ns[sourceConfig.Name] = si.Source

		p.initFieldsFromEnv(sc.FieldsFromEnv)
		p.initFieldsFromPath(sc.FieldsFromPath)

		sourceInvokerChain := buildSourceInvokerChain(sourceConfig.Name, &source.PublishInvoker{}, si.Interceptors)
		productFunc := func(e api.Event) api.Result {
			p.fillEventMetaAndHeader(e, *sourceConfig)

			result := sourceInvokerChain.Invoke(source.Invocation{
				Event: e,
				Queue: q,
			})

			if result.Status() == api.DROP {
				p.info.EventPool.Put(e)
			}
			if result.Status() == api.FAIL {
				log.Error("source to queue failed: %s", result.Error())
			}
			return result
		}
		go si.Source.ProductLoop(productFunc)

	}
}

func (p *Pipeline) initFieldsFromEnv(fieldsFromEnv map[string]string) {
	if len(fieldsFromEnv) == 0 {
		return
	}

	for k, envKey := range fieldsFromEnv {
		val := os.Getenv(envKey)
		if val == "" {
			log.Error("init fieldsFromEnv %s failed, env %s value is null", k, envKey)
			continue
		}
		p.envMap[k] = val
	}
}

func (p *Pipeline) initFieldsFromPath(fieldsFromPath map[string]string) {
	if len(fieldsFromPath) == 0 {
		return
	}

	for k, pathKey := range fieldsFromPath {
		out, err := ioutil.ReadFile(pathKey)
		if err != nil {
			log.Error("init fieldsFromPath %s failed, read file %s err: %v", k, pathKey, err)
			continue
		}

		size := len(out)
		if size > fieldsFromPathMaxBytes {
			log.Error("init fieldsFromPath %s failed, file size is: %d, which exceeds the maximum limit of %d", k, size, fieldsFromPathMaxBytes)
			continue
		}

		str := string(out)
		replacer := strings.NewReplacer("\n", "", "\r", "")
		str = replacer.Replace(str)

		p.pathMap[k] = str
	}
}

func (p *Pipeline) fillEventMetaAndHeader(e api.Event, config source.Config) {
	// add meta fields
	e.Meta().Set(event.SystemProductTimeKey, time.Now())
	e.Meta().Set(event.SystemPipelineKey, p.name)
	e.Meta().Set(event.SystemSourceKey, config.Name)
	e.Meta().Set(FieldsUnderRoot, config.FieldsUnderRoot)
	e.Meta().Set(FieldsUnderKey, config.FieldsUnderKey)

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
		e.Fill(e.Meta(), header, e.Body())
	}
	// add header source fields
	AddSourceFields(header, config.Fields, config.FieldsUnderRoot, config.FieldsUnderKey)

	// add header source fields from env
	AddSourceFields(header, p.envMap, config.FieldsUnderRoot, config.FieldsUnderKey)

	// add header source fields from file
	AddSourceFields(header, p.pathMap, config.FieldsUnderRoot, config.FieldsUnderKey)
}

func AddSourceFields(header map[string]interface{}, fields map[string]interface{}, underRoot bool, fieldsKey string) {
	if len(fields) == 0 {
		return
	}
	if underRoot {
		for k, v := range fields {
			header[k] = v
		}
		return
	}

	// Copy the fields field to avoid being updated later
	fieldsCopy := make(map[string]interface{})
	for k, v := range fields {
		fieldsCopy[k] = v
	}
	if originFields, exist := header[fieldsKey]; exist {
		if originFieldsMap, convert := originFields.(map[string]interface{}); convert {
			for k, v := range fieldsCopy {
				originFieldsMap[k] = v
			}
		}
		return
	}
	header[fieldsKey] = fieldsCopy
}

func buildSourceInvokerChain(sourceName string, invoker source.Invoker, interceptors []source.Interceptor) source.Invoker {
	l := len(interceptors)
	if l == 0 {
		return invoker
	}
	last := invoker

	var interceptorChainName strings.Builder
	interceptorChainName.WriteString("source->")

	// sort interceptor
	sortableInterceptor := source.SortableInterceptor(interceptors)
	sortableInterceptor.Sort()
	for i := 0; i < l; i++ {
		tempInterceptor := sortableInterceptor[l-1-i]
		if extension, ok := tempInterceptor.(interceptor.Extension); ok {
			belongTo := extension.BelongTo()
			// calling len(belongTo) cannot be ignored
			if len(belongTo) > 0 && !util.Contain(sourceName, belongTo) {
				continue
			}
		}
		next := last
		last = &source.AbstractInvoker{
			DoInvoke: func(invocation source.Invocation) api.Result {
				return tempInterceptor.Intercept(next, invocation)
			},
		}

		interceptorChainName.WriteString(sortableInterceptor[i].String())
		interceptorChainName.WriteString("->")
	}

	interceptorChainName.WriteString("queue")
	log.Info("source %s interceptor chain: %s", sourceName, interceptorChainName.String())

	return last
}

func collectComponentDependencySinkInterceptors(component api.Component) []sink.Interceptor {
	sis := make([]sink.Interceptor, 0)
	interceptors := collectComponentDependencyInterceptors(component)
	for _, i := range interceptors {
		if si, ok := i.(sink.Interceptor); ok {
			sis = append(sis, si)
		}
	}
	return sis
}

func collectComponentDependencyInterceptors(component api.Component) []api.Interceptor {
	interceptors := make([]api.Interceptor, 0)
	if extensionComponent, ok := component.(api.ExtensionComponent); ok {
		dependencyInterceptors := extensionComponent.DependencyInterceptors()
		interceptors = append(interceptors, dependencyInterceptors...)
	}
	return interceptors
}

func (p *Pipeline) survive() {
	p.countDown.Add(1)
	defer p.countDown.Done()

	for {
		select {
		case <-p.done:
			return
		case b := <-p.info.SurviveChan:
			result := p.next()(b)
			p.afterSinkConsumer(b, result)
		}
	}
}

// round robin
func (p *Pipeline) next() api.OutFunc {
	size := len(p.retryOutFuncs)
	if size == 1 {
		return p.retryOutFuncs[0]
	}
	p.index++
	return p.retryOutFuncs[int(p.index)%size]
}

func (p *Pipeline) reportMetricWithCode(code string, component api.Component, eventType eventbus.ComponentEventType) {
	var name string
	a := strings.Split(code, "/")
	i := len(a)
	if i < 3 {
		name = ""
	} else {
		name = a[i-1]
	}
	p.reportMetric(name, component, eventType)
}

func (p *Pipeline) reportMetric(name string, component api.Component, eventType eventbus.ComponentEventType) {
	eventbus.Publish(eventbus.ComponentBaseTopic, eventbus.ComponentBaseMetricData{
		EventType:    eventType,
		PipelineName: p.name,
		EpochTime:    p.epoch.StartTime,
		Config: eventbus.ComponentBaseConfig{
			Name:     name,
			Type:     component.Type(),
			Category: component.Category(),
		},
	})
}
