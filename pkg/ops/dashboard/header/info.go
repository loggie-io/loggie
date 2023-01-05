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

package header

import (
	"context"
	"fmt"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/gui"
	"github.com/rivo/tview"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
)

type Info struct {
	gui *gui.Gui

	*tview.TextView
	text string
}

func NewInfo(g *gui.Gui) *Info {
	h := &Info{
		gui:      g,
		TextView: tview.NewTextView(),
	}

	h.setData()
	h.display()

	return h
}

func (h *Info) display() {
	h.SetDynamicColors(true)
	h.SetText(h.text)
}

func (h *Info) queryState() {
	nodeList, err := h.gui.K8sClient.KubeClient.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		h.text = err.Error() + "\n"
		return
	}
	h.gui.GlobalStat.K8sNodeCount = len(nodeList.Items)

	icpList, err := h.gui.K8sClient.LgcClient.LoggieV1beta1().Interceptors().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		h.text = err.Error() + "\n"
		return
	}
	h.gui.GlobalStat.InterceptorCount = len(icpList.Items)

	sinkList, err := h.gui.K8sClient.LgcClient.LoggieV1beta1().Sinks().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		h.text = err.Error() + "\n"
		return
	}
	h.gui.GlobalStat.SinkCount = len(sinkList.Items)
}

func (h *Info) setData() {
	h.queryState()

	var out strings.Builder

	stat := h.gui.GlobalStat
	out.WriteString("\n")
	out.WriteString(fmt.Sprintf("[lightgreen]Kubernetes Version: [white]%s\n", stat.K8sVersion))
	out.WriteString(fmt.Sprintf("[lightgreen]Loggie Version: [white]%s\n", stat.LoggieVersion))

	h.text = h.text + out.String()
}
