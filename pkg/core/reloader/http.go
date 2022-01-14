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

package reloader

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
)

const handleReadPipelineConfigPath = "/api/v1/reload/config"

func (r *Reloader) initHttp() {
	http.HandleFunc(handleReadPipelineConfigPath, r.readPipelineConfigHandler)
}

func (r *Reloader) readPipelineConfigHandler(writer http.ResponseWriter, request *http.Request) {
	matches, err := filepath.Glob(r.config.ConfigPath)
	if err != nil {
		log.Info("glob match path %s error: %v", r.config.ConfigPath, err)
		return
	}

	var data []byte
	for _, m := range matches {
		s, err := os.Stat(m)
		if err != nil {
			log.Info("stat file %s error: %v", m, err)
			return
		}
		if s.IsDir() {
			continue
		}

		content, err := ioutil.ReadFile(m)
		if err != nil {
			log.Warn("read config error. err: %v", err)
			return
		}

		split := "---\n"
		data = append(data, []byte(split)...)
		data = append(data, content...)
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(data)
}
