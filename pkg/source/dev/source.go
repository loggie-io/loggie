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
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/log"
	"loggie.io/loggie/pkg/core/source/abstract"
	"loggie.io/loggie/pkg/pipeline"
)

const Type = "dev"

func init() {
	abstract.SourceRegister(Type, makeSource)
}

func makeSource(info pipeline.Info) abstract.SourceConvert {
	return &Dev{
		Source: abstract.ExtendsAbstractSource(info, Type),
		stop:   info.Stop,
	}
}

type Dev struct {
	*abstract.Source
	stop bool
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
		e := d.Event()
		e.Fill(e.Meta(), header, content)
		productFunc(e)
	}
}

func (d *Dev) DoStart(context api.Context) {
	log.Info("%s override start!", d.String())
}
