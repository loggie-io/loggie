/*
Copyright 2022 Loggie Authors

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

package schema

import (
	"fmt"
	eventer "github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/eventops"
	timeutil "github.com/loggie-io/loggie/pkg/util/time"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

const (
	Type = "schema"

	defaultTsLayout = "2006-01-02T15:04:05.000Z"
)

func init() {
	pipeline.Register(api.INTERCEPTOR, Type, makeInterceptor)
}

func makeInterceptor(info pipeline.Info) api.Component {
	return &Interceptor{
		config: &Config{},
	}
}

type Interceptor struct {
	config *Config
}

func (icp *Interceptor) Config() interface{} {
	return icp.config
}

func (icp *Interceptor) Category() api.Category {
	return api.INTERCEPTOR
}

func (icp *Interceptor) Type() api.Type {
	return Type
}

func (icp *Interceptor) String() string {
	return fmt.Sprintf("%s/%s", icp.Category(), icp.Type())
}

func (icp *Interceptor) Init(context api.Context) error {
	return nil
}

func (icp *Interceptor) Start() error {
	return nil
}

func (icp *Interceptor) Stop() {
}

func (icp *Interceptor) Intercept(invoker source.Invoker, invocation source.Invocation) api.Result {
	event := invocation.Event
	header := event.Header()

	if icp.config.AddMeta != nil {
		addMetaTimestamp(header, event.Meta(), icp.config)
		addMetaPipelineName(header, event.Meta(), icp.config)
		addMetaSourceName(header, event.Meta(), icp.config)
	}

	if len(icp.config.Remap) > 0 {
		remap(event, icp.config)
	}

	return invoker.Invoke(invocation)
}

func addMetaTimestamp(header map[string]interface{}, meta api.Meta, config *Config) {
	conf := config.AddMeta.Timestamp
	if conf.Key == "" {
		return
	}

	timestamp, exist := meta.Get(eventer.SystemProductTimeKey)
	if !exist {
		return
	}

	t, ok := timestamp.(time.Time)
	if !ok {
		return
	}

	layout := conf.Layout
	if layout == "" {
		layout = defaultTsLayout
	}

	// conf.Location could be "" or "UTC" or "Local"
	// default "" indicate "UTC"
	out, err := timeutil.Format(t, conf.Location, layout)
	if err != nil {
		log.Warn("time format system product timestamp err: %+v", err)
		return
	}
	header[conf.Key] = out
}

func addMetaPipelineName(header map[string]interface{}, meta api.Meta, config *Config) {
	if config.AddMeta.PipelineName.Key == "" {
		return
	}

	pipelineName, exist := meta.Get(eventer.SystemPipelineKey)
	if !exist {
		return
	}

	header[config.AddMeta.PipelineName.Key] = pipelineName
}

func addMetaSourceName(header map[string]interface{}, meta api.Meta, config *Config) {
	if config.AddMeta.SourceName.Key == "" {
		return
	}

	sourceName, exist := meta.Get(eventer.SystemSourceKey)
	if !exist {
		return
	}

	header[config.AddMeta.SourceName.Key] = sourceName
}

func remap(event api.Event, config *Config) {
	for k, v := range config.Remap {
		eventops.Move(event, k, v.Key)
	}
}

func (icp *Interceptor) Order() int {
	return icp.config.Order
}

func (icp *Interceptor) BelongTo() (componentTypes []string) {
	return icp.config.BelongTo
}

func (icp *Interceptor) IgnoreRetry() bool {
	return true
}
