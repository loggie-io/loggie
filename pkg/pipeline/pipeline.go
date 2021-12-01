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
	"github.com/pkg/errors"
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/cfg"
	"loggie.io/loggie/pkg/core/context"
	"loggie.io/loggie/pkg/core/event"
	"loggie.io/loggie/pkg/core/interceptor"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/core/queue"
	"loggie.io/loggie/pkg/core/sink"
	"loggie.io/loggie/pkg/core/source"
	"loggie.io/loggie/pkg/sink/codec"
	"loggie.io/loggie/pkg/util"
	"strings"
	"sync"
	"time"
)

const (
	fieldsUnderRootKey = event.PrivateKeyPrefix + "FieldsUnderRoot"
	fieldsUnderKeyKey  = event.PrivateKeyPrefix + "FieldsUnderKey"
)

type Pipeline struct {
	name          string
	config        Config
	done          chan struct{}
	info          Info
	r             *RegisterCenter
	ns            map[string]api.Source // key:name|value:source
	q             api.Queue
	outChans      []chan api.Batch
	countDown     sync.WaitGroup
	retryOutFuncs []api.OutFunc
	index         uint32
	epoch         Epoch
}

func NewPipeline() *Pipeline {
	registerCenter := NewRegisterCenter()
	return &Pipeline{
		done: make(chan struct{}),
		info: Info{
			Stop:        false,
			R:           registerCenter,
			SurviveChan: make(chan api.Batch, 3),
		},
		r: registerCenter,
	}
}

func (p *Pipeline) Stop() {
	p.info.Stop = true
	// 0. stop source product
	p.stopSourceProduct()
	// 1. stop queue
	p.stopQueue()
	// 2. stop sink consumer
	p.stopSinkConsumer()
	// 3. stop component
	// care about stopped components
	p.stopComponents()
	// 4. stop listeners
	p.stopListeners()
	// 5. clean data(out chan、registry center、and so on)
	p.cleanData()

}

func (p *Pipeline) stopSinkConsumer() {
	close(p.done)
	p.countDown.Wait()
}

func (p *Pipeline) stopSourceProduct() {
	for name, s := range p.ns {
		s.Stop()
		p.r.removeComponent(s.Type(), s.Category(), name)
	}
}

func (p *Pipeline) stopQueue() {
	p.q.Stop()
	p.r.removeComponent(p.q.Type(), p.q.Category(), "default") // TODO
}

func (p *Pipeline) stopComponents() {
	log.Info("start stopComponents,timeout: %ds", p.config.CleanDataTimeout/time.Second)
	for name, v := range p.r.nameComponents {
		// async stop with timeout
		n := name
		c := v
		util.AsyncRunWithTimeout(func() {
			c.Stop()
			delete(p.r.nameComponents, n)
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
	// clean out chan
	p.cleanOutChan()
	// clean registry center
	p.r.cleanData()
	// clean pipeline
	p.ns = nil
	p.q = nil
	p.outChans = nil
	p.r = nil
}

func (p *Pipeline) cleanOutChan() {
	for _, outChan := range p.outChans {
		out := outChan
		if len(out) == 0 {
			continue
		}
		go p.consumerOutChanAndDrop(out)
	}
}

func (p *Pipeline) consumerOutChanAndDrop(out chan api.Batch) {
	after := time.NewTimer(p.config.CleanDataTimeout)
	defer after.Stop()
	for {
		select {
		case <-after.C:
			return
		case b := <-out:
			// drop
			p.finalizeBatch(b)
		case b := <-p.info.SurviveChan:
			// drop
			p.finalizeBatch(b)
		}
	}
}

func (p *Pipeline) Start(pipelineConfig Config) {
	p.init(pipelineConfig)
	// 0. check:
	// 		1. pipeline name check
	// 		2. mark unused interceptor
	// 		3. mark unused queue
	// 		4. check sink: sink can only consumer one queue...
	// 		5. check queue
	// 		6. check source

	// 1. start interceptor
	p.startInterceptor(pipelineConfig.Interceptors)
	// 2. start sink
	p.startSink(pipelineConfig.Sink)
	// 3. start source
	p.startSource(pipelineConfig.Sources)
	// 4. start queue
	p.startQueue(*pipelineConfig.Queue)
	// 5. start sink consumer
	p.startSinkConsumer(pipelineConfig.Sink)
	// 6. start source product
	p.startSourceProduct(pipelineConfig.Sources)

	go p.survive()
	log.Info("pipeline start with epoch: %+v", p.epoch)
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

	// init event pool
	p.info.EventPool = event.NewDefaultPool(pipelineConfig.Queue.BatchSize * (p.info.SinkCount + 1))
}

func (p *Pipeline) startInterceptor(interceptorConfigs []interceptor.Config) {
	for _, iConfig := range interceptorConfigs {
		ctx := context.NewContext(iConfig.Name, api.Type(iConfig.Type), api.INTERCEPTOR, iConfig.Properties)
		p.startComponent(ctx)
	}
}

func (p *Pipeline) startQueue(queueConfig queue.Config) {
	ctx := context.NewContext(queueConfig.Name, api.Type(queueConfig.Type), api.QUEUE, queueConfig.Properties)
	p.startComponent(ctx)
	q := p.r.LoadQueue(api.Type(queueConfig.Type), queueConfig.Name)
	p.q = q
	p.outChans = append(p.outChans, q.OutChan())
}

func (p *Pipeline) startComponent(ctx api.Context) {
	component, _ := GetWithType(ctx.Category(), ctx.Type(), p.info)
	p.startWithComponent(component, ctx)
}

func (p *Pipeline) startWithComponent(component api.Component, ctx api.Context) {
	// unpack config from properties
	err := cfg.UnpackAndDefaults(ctx.Properties(), component.Config())
	if err != nil {
		log.Panic("unpack component %s/%s error: %v", component.Category(), component.Type(), err)
	}

	component.Init(ctx)
	component.Start()
	p.r.Register(component, ctx.Name())
}

func (p *Pipeline) afterSinkConsumer(b api.Batch, result api.Result) {
	// commit to source and release batch
	if result.Status() == api.SUCCESS || result.Status() == api.DROP {
		p.finalizeBatch(b)
	}
	if result.Status() == api.FAIL {
		log.Error("consumer batch fail,err: %s", result.Error())
	}
}

// commit to source and release batch
func (p *Pipeline) finalizeBatch(batch api.Batch) {
	//p.s.Commit(batch)

	nes := make(map[string][]api.Event)
	events := batch.Events()
	l := len(events)
	for _, e := range events {
		sourceName := e.Source()
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
	return cfg.UnpackDefaultsAndValidate(ctx.Properties(), component.Config())
}

func (p *Pipeline) validate(pipelineConfig *Config) error {
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
	ctx = context.NewContext(sinkConfig.Name, api.Type(sinkConfig.Type), api.SINK, sinkConfig.Properties)
	if err := p.validateComponent(ctx); err != nil {
		return err
	}

	unique := make(map[string]struct{})
	for _, sourceConfig := range pipelineConfig.Sources {
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

func (p *Pipeline) startSink(sinkConfigs *sink.Config) {
	p.retryOutFuncs = make([]api.OutFunc, 0)
	ctx := context.NewContext(sinkConfigs.Name, api.Type(sinkConfigs.Type), api.SINK, sinkConfigs.Properties)

	// get codec config
	codecConf := sinkConfigs.Codec

	// init codec
	cod, ok := codec.Get(codecConf.Type)
	if !ok {
		log.Panic("codec %s cannot be found", codecConf.Type)
	}
	if conf, ok := cod.(api.Config); ok {
		err := cfg.UnpackAndDefaults(codecConf.CommonCfg, conf.Config())
		if err != nil {
			log.Panic("unpack codec config error: %+v", err)
		}
	}
	cod.Init()

	// set codec to sink
	component, _ := GetWithType(ctx.Category(), ctx.Type(), p.info)
	if si, ok := component.(codec.SinkCodec); ok {
		si.SetCodec(cod)
	}

	p.startWithComponent(component, ctx)
}

func (p *Pipeline) startSinkConsumer(sinkConfig *sink.Config) {
	interceptors := make([]sink.Interceptor, 0)
	for _, inter := range p.r.Components(api.INTERCEPTOR) {
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
	// combine component default interceptors
	interceptors = append(interceptors, collectComponentDependencySinkInterceptors(si.Sink)...)
	interceptors = append(interceptors, collectComponentDependencySinkInterceptors(si.Queue)...)

	invoker := &sink.SubscribeInvoker{}
	sinkInvokerChain := buildSinkInvokerChain(invoker, interceptors, false)
	retrySinkInvokerChain := buildSinkInvokerChain(invoker, interceptors, true)
	outFunc := func(batch api.Batch) api.Result {
		result := sinkInvokerChain.Invoke(sink.Invocation{
			Batch: batch,
			Sink:  si.Sink,
		})
		return result
	}
	retryOutFunc := func(batch api.Batch) api.Result {
		result := retrySinkInvokerChain.Invoke(sink.Invocation{
			Batch: batch,
			Sink:  si.Sink,
		})
		return result
	}
	for i := 0; i < sinkConfig.Parallelism; i++ {
		index := i
		p.retryOutFuncs = append(p.retryOutFuncs, retryOutFunc)
		go p.sinkInvokeLoop(index, si, outFunc)
	}
}

// outfunc may have been combined, but batch has been released in advance
func (p *Pipeline) sinkInvokeLoop(index int, info sink.Info, outFunc api.OutFunc) {
	p.countDown.Add(1)
	s := info.Sink
	log.Info("pipeline sink(%s)-%d invoke loop start", s.String(), index)
	defer func() {
		p.countDown.Done()
		log.Info("pipeline sink(%s)-%d invoke loop stop", s.String(), index)
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
		}
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
	sink.SortableInterceptor(interceptors).Sort()
	// build chain
	for _, ic := range interceptors {
		//filter retry ignore
		if retry {
			if extension, ok := ic.(interceptor.Extension); ok && extension.IgnoreRetry() {
				continue
			}
		}
		tempInterceptor := ic
		next := last
		last = &sink.AbstractInvoker{
			DoInvoke: func(invocation sink.Invocation) api.Result {
				return tempInterceptor.Intercept(next, invocation)
			},
		}

		interceptorChainName.WriteString(tempInterceptor.String())
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

func (p *Pipeline) startSource(sourceConfigs []source.Config) {
	for _, sourceConfig := range sourceConfigs {
		ctx := context.NewContext(sourceConfig.Name, api.Type(sourceConfig.Type), api.SOURCE, sourceConfig.Properties)
		p.startComponent(ctx)
	}
}

func (p *Pipeline) startSourceProduct(sourceConfigs []source.Config) {
	for _, sc := range sourceConfigs {
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

		sourceInvokerChain := buildSourceInvokerChain(sourceConfig.Type, &source.PublishInvoker{}, si.Interceptors)
		productFunc := func(e api.Event) api.Result {
			p.fillEventHeader(e, sourceConfig)

			result := sourceInvokerChain.Invoke(source.Invocation{
				Event: e,
				Queue: q,
			})
			return result
		}
		go si.Source.ProductLoop(productFunc)
	}
	//go p.sourceInvokeLoop(si)
}

func (p *Pipeline) fillEventHeader(e api.Event, config source.Config) {
	header := e.Header()
	// add system fields
	header[event.SystemPipelineKey] = p.name
	header[event.SystemSourceKey] = config.Name
	// add private fields
	header[fieldsUnderRootKey] = config.FieldsUnderRoot
	header[fieldsUnderKeyKey] = config.FieldsUnderKey

	// add source fields
	fields := config.Fields
	if len(fields) > 0 {
		if config.FieldsUnderRoot {
			for k, v := range fields {
				header[k] = v
			}
		} else {
			header[config.FieldsUnderKey] = fields
		}
	}
}

func buildSourceInvokerChain(sourceType string, invoker source.Invoker, interceptors []source.Interceptor) source.Invoker {
	if len(interceptors) == 0 {
		return invoker
	}
	last := invoker

	var interceptorChainName strings.Builder
	interceptorChainName.WriteString("source->")

	// sort interceptor
	source.SortableInterceptor(interceptors).Sort()
	for _, ic := range interceptors {
		if extension, ok := ic.(interceptor.Extension); ok {
			belongTo := extension.BelongTo()
			// calling len(belongTo) cannot be ignored
			if len(belongTo) > 0 && !util.Contain(sourceType, belongTo) {
				continue
			}
		}
		tempInterceptor := ic
		next := last
		last = &source.AbstractInvoker{
			DoInvoke: func(invocation source.Invocation) api.Result {
				return tempInterceptor.Intercept(next, invocation)
			},
		}

		interceptorChainName.WriteString(tempInterceptor.String())
		interceptorChainName.WriteString("->")
	}

	interceptorChainName.WriteString("queue")
	log.Info("source interceptor chain: %s", interceptorChainName.String())

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

func collectComponentDependencySourceInterceptors(component api.Component) []source.Interceptor {
	sis := make([]source.Interceptor, 0)
	interceptors := collectComponentDependencyInterceptors(component)
	for _, i := range interceptors {
		if si, ok := i.(source.Interceptor); ok {
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
