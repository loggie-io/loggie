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

package dev

import (
	"fmt"
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/event"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/pipeline"
)

const Type = "dev"

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	return &Dev{
		stop:      info.Stop,
		eventPool: info.EventPool,
	}
}

type Dev struct {
	name      string
	stop      bool
	eventPool *event.Pool
}

func (d *Dev) Config() interface{} {
	return nil
}

func (d *Dev) Category() api.Category {
	return api.SOURCE
}

func (d *Dev) Type() api.Type {
	return Type
}

func (d *Dev) String() string {
	return fmt.Sprintf("%s/%s", api.SOURCE, Type)
}

func (d *Dev) Init(context api.Context) {
	d.name = context.Name()
}

func (d *Dev) Start() {

}

func (d *Dev) Stop() {

}

func (d *Dev) Product() api.Event {
	return nil
}

func (d *Dev) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", d.String())
	// size: 579 bytes
	content := []byte("sadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasjdfklsadjfklsdfjklsadlfjklajskdljasj")
	//content := "888"
	for !d.stop {
		header := make(map[string]interface{})
		header["offset"] = 888
		header["topic"] = "log-test"
		e := d.eventPool.Get()
		e.Fill(header, content)
		productFunc(e)
	}
}

func (d *Dev) Commit(events []api.Event) {
	d.eventPool.PutAll(events)
}
