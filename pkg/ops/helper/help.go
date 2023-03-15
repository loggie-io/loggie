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
	"github.com/loggie-io/loggie/pkg/control"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/reloader"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/source/file"
	"github.com/loggie-io/loggie/pkg/util/yaml"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	handleHelper              = "/api/v1/help"
	HandleHelperLogCollection = "/api/v1/help/log"
	HandleHelperVersion       = "/version"

	modulePipeline = "pipeline"
	moduleLog      = "log"

	queryDetail   = "detail"
	queryPipeline = "pipeline"
	querySource   = "source"
	queryStatus   = "status"

	statusPending = "pending"
)

var helperIns *Helper

type Helper struct {
	controller *control.Controller
}

func Setup(controller *control.Controller) {
	helperIns = &Helper{
		controller: controller,
	}

	http.HandleFunc(handleHelper, helperIns.helperHandler)
	http.HandleFunc(HandleHelperLogCollection, helperIns.helperLogCollectionHandler)
	http.HandleFunc(HandleHelperVersion, helperIns.HelperVersionHandler)
}

func (h *Helper) helperHandler(writer http.ResponseWriter, request *http.Request) {
	detail := request.URL.Query().Get(queryDetail)

	var sb strings.Builder

	if detail == "" {
		sb.WriteString(printUsage())
	}

	if detail == "" || detail == modulePipeline {
		// print pipelines info
		sb.WriteString(pipelineStatus(h.controller))

		// check pipeline configurations consistency
		sb.WriteString(diffPipes(request))
	}

	if detail == "" || detail == moduleLog {
		sb.WriteString(printLogCollectionStatus(h.controller, request))
	}

	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte(sb.String()))
}

func printUsage() string {
	var sb strings.Builder

	sb.WriteString(SprintfWithLF("--------- Usage: -----------------------"))
	sb.WriteString(SprintfWithLF("|--- view details: /api/v1/help?detail=<module>, module is one of: %s/%s", modulePipeline, moduleLog))
	sb.WriteString(SprintfWithLF("|--- query by pipeline name: /api/v1/help?pipeline=<name>"))
	sb.WriteString(SprintfWithLF("|--- query by source name: /api/v1/help?source=<name>"))
	sb.WriteString(SprintfWithLF("|--- query by version: /version"))

	sb.WriteString(CRLF())
	return sb.String()
}

func pipelineStatus(controller *control.Controller) string {
	var sb strings.Builder

	sb.WriteString(SprintfWithLF("--------- Pipeline Status: --------------"))

	length := len(controller.CurrentConfig.Pipelines)
	sb.WriteString(SprintfWithLF("all %d pipelines running", length))
	if length == 0 {
		return sb.String()
	}

	for _, p := range controller.CurrentConfig.Pipelines {
		var sourceNames []string
		for _, s := range p.Sources {
			sourceNames = append(sourceNames, s.Name)
		}
		sb.WriteString(SprintfWithLF("  * pipeline: %s, sources: %+v", p.Name, sourceNames))
	}
	sb.WriteString(CRLF())

	return sb.String()
}

func diffPipes(request *http.Request) string {
	pipelineQuery := request.URL.Query().Get(queryPipeline)
	sourceQuery := request.URL.Query().Get(querySource)

	var sb strings.Builder

	cfgInPath, diffs, _, _ := reloader.DiffPipelineConfigs(func(s os.FileInfo) bool {
		return false
	})
	if len(diffs) == 0 {
		sb.WriteString(SprintfWithLF("✅ pipeline configurations consistency check passed"))
	} else {
		sb.WriteString(SprintfWithLF("❌ pipeline configurations diff:"))
		for _, d := range diffs {
			sb.WriteString(SprintfWithLF("%s", d))
		}
		sb.WriteString(SprintfWithLF("please check this again in the next reload period"))
	}
	sb.WriteString(CRLF())

	// search pipelineName or sourceName in pipeline configurations
	if pipelineQuery == "" && sourceQuery == "" {
		out, err := yaml.Marshal(cfgInPath)
		if err != nil {
			log.Error("yaml marshal pipeline configuration error: %v", err)
		}
		sb.Write(out)
		sb.WriteString(CRLF())

	} else {
		result := queryPipelineConfig(cfgInPath, pipelineQuery, sourceQuery)
		if len(result) > 0 {
			out, err := yaml.Marshal(result)
			if err != nil {
				log.Error("yaml marshal pipeline configuration error: %v", err)
			}
			sb.Write(out)
			sb.WriteString(CRLF())
		}
	}

	sb.WriteString(SprintfWithLF("| more details:"))
	sb.WriteString(SprintfWithLF("|--- pipelines configuration in the path ref: %s", reloader.HandleReadPipelineConfigPath))
	sb.WriteString(SprintfWithLF("|--- current running pipelines configuration ref: %s", control.HandleCurrentPipelines))
	sb.WriteString(CRLF())

	return sb.String()
}

func queryPipelineConfig(cfgInPath *control.PipelineConfig, pipelineQuery string, sourceQuery string) map[string]pipeline.Config {
	result := make(map[string]pipeline.Config)

	setResult := func(pipData pipeline.Config, srcData ...*source.Config) {
		pip, ok := result[pipData.Name]
		if ok {
			// add sources
			pip.Sources = append(pip.Sources, srcData...)
			return
		}

		var data = pipData
		if len(srcData) > 0 {
			data = pipeline.Config{
				Name:         pipData.Name,
				Sources:      srcData,
				Interceptors: pipData.Interceptors,
				Sink:         pipData.Sink,
			}
		}
		result[data.Name] = data
	}

	for _, pip := range cfgInPath.Pipelines {
		if pipelineQuery != "" && sourceQuery != "" {
			if pip.Name != pipelineQuery {
				continue
			}

			for _, src := range pip.Sources {
				if src.Name == sourceQuery {
					setResult(pip, src)
				}
			}

		} else if pipelineQuery != "" {
			if pip.Name == pipelineQuery {
				setResult(pip)
			}

		} else if sourceQuery != "" {
			for _, src := range pip.Sources {
				if src.Name == sourceQuery {
					setResult(pip, src)
				}
			}
		}
	}

	return result
}

func printLogCollectionStatus(controller *control.Controller, request *http.Request) string {
	statusQuery := request.URL.Query().Get(queryStatus)

	if !hasFileSource(controller) {
		return CRLF()
	}

	var sb strings.Builder
	sb.WriteString(SprintfWithLF("--------- Log Collection Status: ---------"))

	// Glob path matched log files
	activeCount, inActiveCount, resultInfo := fileInfoMetrics(request)
	sb.WriteString(CRLF())
	sb.WriteString(SprintfWithLF("all activeFdCount: %d, inActiveFdCount: %d", activeCount, inActiveCount))
	if len(resultInfo) > 0 {
		sb.WriteString(CRLF())
		sb.WriteString(SprintfWithLF("> pipeline * source - filename | progress(offset/size) | modify"))
	}
	for pip, info := range resultInfo {
		// print pipeline name
		sb.WriteString(SprintfWithLF("  > %s", pip))
		for _, in := range info {
			// print source name
			sb.WriteString(SprintfWithLF("    * %s", in.SourceName))

			// sort by collect progress
			sort.Slice(in.FileInfos, func(i, j int) bool {
				return float64(in.FileInfos[i].Offset)/float64(in.FileInfos[i].Size) < float64(in.FileInfos[j].Offset)/float64(in.FileInfos[j].Size)
			})

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
					if f.Size == f.Offset {
						continue
					}

					sb.WriteString(fmtLogFileCollection(f))
					continue
				}

				sb.WriteString(fmtLogFileCollection(f))
				if f.IsIgnoreOlder {
					sb.WriteString(SprintfWithLF(" | ignored "))
				}
				sb.WriteString(CRLF())
			}
		}
	}

	sb.WriteString(CRLF())
	sb.WriteString(SprintfWithLF("| more details:"))
	sb.WriteString(SprintfWithLF("|--- registry storage ref: %s", file.HandlerRegistryPath))
	sb.WriteString(CRLF())

	return sb.String()
}

func fmtLogFileCollection(f eventbus.FileInfo) string {
	return fmt.Sprintf("      - %s | %.2f%%(%d/%d) | %s", f.FileName, float64(f.Offset)/float64(f.Size)*100, f.Offset, f.Size, f.LastModifyTime.Local().Format(time.RFC3339))
}

func hasFileSource(controller *control.Controller) bool {
	for _, pip := range controller.CurrentConfig.Pipelines {
		for _, src := range pip.Sources {
			if src.Type == file.Type {
				return true
			}
		}
	}

	return false
}

func fileInfoMetrics(request *http.Request) (active int, inActive int, metric map[string][]eventbus.WatchMetricData) {
	pipelineQuery := request.URL.Query().Get(queryPipeline)
	sourceQuery := request.URL.Query().Get(querySource)

	resultInfo := make(map[string][]eventbus.WatchMetricData)
	setData := func(pipeName string, data eventbus.WatchMetricData) {
		d, ok := resultInfo[pipeName]
		if ok {
			d = append(d, data)
			resultInfo[pipeName] = d
		} else {
			resultInfo[pipeName] = []eventbus.WatchMetricData{data}
		}
	}

	var activeCount int
	var inActiveCount int

	filesMetrics := file.ExportWatchMetric()
	for _, val := range filesMetrics {

		if pipelineQuery != "" && sourceQuery != "" {
			if val.PipelineName == pipelineQuery && val.SourceName == sourceQuery {
				setData(val.PipelineName, val)
			}

		} else if pipelineQuery != "" {
			if val.PipelineName == pipelineQuery {
				setData(val.PipelineName, val)
			}

		} else if sourceQuery != "" {
			if val.SourceName == sourceQuery {
				setData(val.PipelineName, val)
			}

		} else {
			setData(val.PipelineName, val)
		}

		activeCount = activeCount + val.ActiveFileCount
		inActiveCount = inActiveCount + val.InactiveFdCount
	}

	return activeCount, inActiveCount, resultInfo
}

func CRLF() string {
	return fmt.Sprintf("\n")
}

func SprintfWithLF(format string, a ...interface{}) string {
	return fmt.Sprintf(format+"\n", a...)
}
