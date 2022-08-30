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

package control

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/yaml"
	"net/http"
)

const HandleCurrentPipelines = "/api/v1/controller/pipelines"

func (c *Controller) initHttp() {
	http.HandleFunc(HandleCurrentPipelines, c.currentPipelinesHandler)
}

func (c *Controller) currentPipelinesHandler(writer http.ResponseWriter, request *http.Request) {
	data, err := yaml.Marshal(c.CurrentConfig)
	if err != nil {
		log.Warn("marshal current pipeline config err: %v", err)
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(err.Error()))
		return
	}

	writer.WriteHeader(http.StatusOK)
	writer.Write(data)
}
