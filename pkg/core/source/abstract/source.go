package abstract

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

type Source struct {
	done         chan struct{}
	name         string
	typeName     api.Type
	eventPool    *event.Pool
	pipelineInfo pipeline.Info
	context      api.Context

	EventProvider func() api.Event

	startFunc  func()
	stopFunc   func()
	commitFunc func(events []api.Event)
}

func ExtendsAbstractSource(info pipeline.Info, typeName api.Type) *Source {
	return &Source{
		typeName:     typeName,
		eventPool:    info.EventPool,
		pipelineInfo: info,
		EventProvider: func() api.Event {
			return info.EventPool.Get()
		},
	}
}

// ------------------------------------------------------------------------
//  extension methods
// ------------------------------------------------------------------------

func (as *Source) Name() string {
	return as.name
}

func (as *Source) PipelineName() string {
	return as.pipelineInfo.PipelineName
}

func (as *Source) Epoch() pipeline.Epoch {
	return as.pipelineInfo.Epoch
}

func (as *Source) PipelineInfo() pipeline.Info {
	return as.pipelineInfo
}

func (as *Source) Event() api.Event {
	return as.eventPool.Get()
}

// ------------------------------------------------------------------------
//  implement methods of api.Source
//  do not override
// ------------------------------------------------------------------------

func (as *Source) Category() api.Category {
	return api.SOURCE
}

func (as *Source) Type() api.Type {
	return as.typeName
}

func (as *Source) String() string {
	return fmt.Sprintf("%s/%s/%s/%s", as.PipelineName(), as.Category(), as.Type(), as.Name())
}

func (as *Source) Init(context api.Context) {
	as.name = context.Name()
	as.context = context
}

func (as *Source) Start() {
	log.Info("start source: %s", as.String())
	if as.startFunc != nil {
		as.startFunc()
	}
	log.Info("source has started: %s", as.String())
}

func (as *Source) Stop() {
	log.Info("start stop source: %s", as.String())
	close(as.done)
	if as.stopFunc != nil {
		as.stopFunc()
	}
	log.Info("source has stopped: %s", as.String())
}

func (as *Source) Commit(events []api.Event) {
	if as.commitFunc != nil {
		as.commitFunc(events)
	}
	if len(events) == 0 {
		return
	}
	// release events
	as.eventPool.PutAll(events)
}

// ------------------------------------------------------------------------
//  optional override methods
// ------------------------------------------------------------------------

// Config  A pointer to config or nil should be returned
func (as *Source) Config() interface{} {
	return nil
}

func (as *Source) ProductLoop(productFunc api.ProductFunc) {
}

func (as *Source) DoStart() {

}

func (as *Source) DoStop() {

}

func (as *Source) DoCommit(events []api.Event) {

}

// ------------------------------------------------------------------------
//  internal methods
// 	do not override
// ------------------------------------------------------------------------

func (as *Source) AbstractSource() *Source {
	return as
}

type SourceConvert interface {
	api.Component
	AbstractSource() *Source

	DoStart()
	DoStop()
	DoCommit(events []api.Event)
}

type SourceRegisterFactory func(info pipeline.Info) SourceConvert

func SourceRegister(t api.Type, factory SourceRegisterFactory) {
	pipeline.Register(api.SOURCE, t, func(info pipeline.Info) api.Component {
		convert := factory(info)
		source := convert.AbstractSource()
		source.startFunc = convert.DoStart
		source.stopFunc = convert.DoStop
		source.commitFunc = convert.DoCommit
		return convert
	})
}
