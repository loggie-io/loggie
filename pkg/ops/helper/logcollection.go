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

package helper

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"math"
	"net/http"
	"time"
)

type LogCollectionStatus struct {
	FdStatus   FdStatus   `json:"fdStatus"`
	FileStatus FileStatus `json:"fileStatus"`
}

type FdStatus struct {
	ActiveFdCount   int `json:"activeFdCount"`
	InActiveFdCount int `json:"inActiveFdCount"`
}

type FileStatus struct {
	PipelineStat map[string]Source `json:"pipeline"`
}

type Source struct {
	SourceStat map[string]SourceDetails `json:"source"`
}

type SourceDetails struct {
	Paths  []string     `json:"paths"`
	Detail []FileDetail `json:"detail"`
}

type FileDetail struct {
	FileName string `json:"filename"`
	Offset   int64  `json:"offset"`
	Size     int64  `json:"size"`
	Modify   int64  `json:"modify"`
	Ignored  bool   `json:"ignored"`
}

func (f *FileStatus) SetStatus(pipeline string, source string, paths []string, detail FileDetail) {
	pip, ok := f.PipelineStat[pipeline]
	if !ok { // add a new pipeline status
		f.PipelineStat[pipeline] = Source{
			SourceStat: map[string]SourceDetails{
				source: {
					Paths:  paths,
					Detail: []FileDetail{detail},
				},
			},
		}
		return
	}

	// pipeline is exist, add a new source status
	srcDetails, ok := pip.SourceStat[source]
	if !ok {
		pip.SourceStat[source] = SourceDetails{
			Paths:  paths,
			Detail: []FileDetail{detail},
		}
		return
	}

	// source is exist, append file collection status
	srcDetails.Detail = append(srcDetails.Detail, detail)
	pip.SourceStat[source] = srcDetails
	return
}

func (d *FileDetail) FmtDetails() string {
	percent := math.Floor(float64(d.Offset)/float64(d.Size)*10000) / 100
	result := fmt.Sprintf("%.2f%% (%d/%d) | %s", percent, d.Offset, d.Size, time.UnixMilli(d.Modify).Local().Format(time.RFC3339))
	return result
}

func (h *Helper) helperLogCollectionHandler(writer http.ResponseWriter, request *http.Request) {

	result := &LogCollectionStatus{}

	statusQuery := request.URL.Query().Get(queryStatus)
	pipelineQuery := request.URL.Query().Get(queryPipeline)

	if !hasFileSource(h.controller) {
		writer.Write([]byte("No file source in pipelines"))
		return
	}

	// Glob path matched log files
	activeCount, inActiveCount, resultInfo := fileInfoMetrics(request)
	fdStatus := FdStatus{
		ActiveFdCount:   activeCount,
		InActiveFdCount: inActiveCount,
	}
	result.FdStatus = fdStatus

	if len(resultInfo) == 0 {
		resultReturn(writer, result)
		return
	}

	fileStatus := FileStatus{
		PipelineStat: make(map[string]Source),
	}
	for pip, info := range resultInfo {
		if pipelineQuery != "" && pipelineQuery != pip {
			continue
		}

		for _, in := range info {
			// print file status details
			for _, f := range in.FileInfos {
				// Only show log files which is being collected (including 0 progress),
				// ignore the collection progress 100%, NaN% and ignored
				if statusQuery == statusPending {
					if f.IsIgnoreOlder {
						continue
					}
					if f.Size == 0 {
						continue
					}
					if f.Offset >= f.Size {
						continue
					}
				}

				detail := FileDetail{
					FileName: f.FileName,
					Offset:   f.Offset,
					Size:     f.Size,
					Modify:   f.LastModifyTime.UnixMilli(),
					Ignored:  f.IsIgnoreOlder,
				}
				fileStatus.SetStatus(pip, in.SourceName, in.Paths, detail)
			}
		}
	}
	result.FileStatus = fileStatus

	resultReturn(writer, result)
}

func resultReturn(writer http.ResponseWriter, result interface{}) {
	out, err := jsoniter.Marshal(result)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(err.Error()))
		return
	}

	writer.Write(out)
}
