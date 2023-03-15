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

package ops

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/control"
	"github.com/loggie-io/loggie/pkg/core/global"
	"net/http"
	"strings"
)

const (
	HandleVersion = "/version"
)

var VersionIns *Version

type Version struct {
	controller *control.Controller
}

func Setup(controller *control.Controller) {
	VersionIns = &Version{
		controller: controller,
	}
	http.HandleFunc(HandleVersion, VersionIns.VersionHandler)
}

func (h *Version) VersionHandler(writer http.ResponseWriter, request *http.Request) {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf((global.GetVersion() + "\n")))
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte(sb.String()))

}
