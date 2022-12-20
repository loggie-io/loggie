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

package file

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/hpcloud/tail"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/persistence"
)

var once sync.Once

const (
	handlerProxyPath    = "/api/v1/source/file/proxy"
	HandlerRegistryPath = "/api/v1/source/file/registry"
)

func (s *Source) HandleHttp() {
	once.Do(func() {
		log.Info("handle http func: %+v", handlerProxyPath)
		http.HandleFunc(handlerProxyPath, proxyFileHandler)

		log.Info("handle http func: %+v", HandlerRegistryPath)
		http.HandleFunc(HandlerRegistryPath, s.registryHandler)
	})
}

func proxyFileHandler(writer http.ResponseWriter, request *http.Request) {

	filename := request.URL.Query().Get("filename")
	if filename == "" {
		writer.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(writer, "param filename is missing")
		return
	}

	followParam := request.URL.Query().Get("follow")
	if followParam == "" {
		followParam = "false"
	}
	follow, err := strconv.ParseBool(followParam)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(writer, "param follow is validate, err: %v", err)
		return
	}

	tailLineParam := request.URL.Query().Get("tailLines")
	tailLine, err := strconv.ParseUint(tailLineParam, 10, 0)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(writer, "param tailLine is validate, err: %v", err)
		return
	}

	f, err := os.Open(filename)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(writer, "open file err: %v", err)
		return
	}
	defer f.Close()

	offset, err := findTailLineStartIndex(f, int64(tailLine))
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(writer, "findTailLineStartIndex error: %v", err)
		return
	}
	tailer, err := tail.TailFile(filename, tail.Config{
		Location: &tail.SeekInfo{
			Offset: offset,
			Whence: io.SeekStart,
		},
		Follow: follow,
	})
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(writer, "tail file error: %v", err)
		return
	}
	for line := range tailer.Lines {
		writer.Write([]byte(line.Text + "\n"))
	}
}

const (
	// blockSize is the block size used in tail.
	blockSize = 1024
)

var (
	// eol is the end-of-line sign in the log.
	eol = []byte{'\n'}
)

func findTailLineStartIndex(f io.ReadSeeker, n int64) (int64, error) {
	if n < 0 {
		return 0, nil
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	var left, cnt int64
	buf := make([]byte, blockSize)
	for right := size; right > 0 && cnt <= n; right -= blockSize {
		left = right - blockSize
		if left < 0 {
			left = 0
			buf = make([]byte, right)
		}
		if _, err := f.Seek(left, io.SeekStart); err != nil {
			return 0, err
		}
		if _, err := f.Read(buf); err != nil {
			return 0, err
		}
		cnt += int64(bytes.Count(buf, eol))
	}
	for ; cnt > n; cnt-- {
		idx := bytes.Index(buf, eol) + 1
		buf = buf[idx:]
		left += int64(idx)
	}
	return left, nil
}

const (
	jsonFormat = "json"
	textFormat = "text"
)

func (s *Source) registryHandler(writer http.ResponseWriter, request *http.Request) {

	format := request.URL.Query().Get("format")
	if format != jsonFormat && format != textFormat {
		format = jsonFormat
	}

	pretty := false
	prettyStr := request.URL.Query().Get("pretty")
	if prettyStr == "true" {
		pretty = true
	}

	db := persistence.GetOrCreateShareDbHandler(s.config.DbConfig)
	registry := db.FindAll()

	var out []byte
	var err error
	if format == jsonFormat {
		if pretty {
			out, err = json.MarshalIndent(registry, "", "  ")
		} else {
			out, err = json.Marshal(registry)
		}

	} else {
		for _, r := range registry {
			o := []byte(fmt.Sprintf("%+v \n", r))
			out = append(out, o...)
		}
	}

	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		log.Warn("marshal registry error: %v", err)
		return
	}

	writer.WriteHeader(http.StatusOK)
	writer.Write(out)
}
