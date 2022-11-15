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

package loggie

import (
	"context"
	"fmt"
	"github.com/gdamore/tcell/v2"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/gui"
	"github.com/loggie-io/loggie/pkg/util/time"
	"github.com/rivo/tview"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"strings"
)

const AgentPanelName = "Agent"

var (
	cellColor = gui.ColorYellow
)

type AgentPanel struct {
	gui *gui.Gui
	*tview.Table
	filterWord string

	states []AgentState
}

type AgentState struct {
	PodName   string
	Namespace string
	Status    string
	Restarts  int32
	Created   metav1.Time
	CPU       string
	Memory    string
	IP        string
	Node      string
}

type PodMetric struct {
	CPU    string
	Memory string
}

func NewAgentView(g *gui.Gui) *AgentPanel {
	l := &AgentPanel{
		gui:   g,
		Table: tview.NewTable().SetSelectable(true, false).Select(0, 0).SetFixed(1, 1),
	}

	l.SetTitleAlign(tview.AlignCenter)
	l.Table.SetSelectedStyle(gui.NewTableSelectedStyle(cellColor))
	l.SetBorder(true)
	l.SetData()
	l.SetKeybinding(g)
	g.Nav.AddKeyBindingsNavWithKey(AgentPanelName, "", gui.KeyTab, gui.KeyEnter, gui.KeyO, gui.KeyQ, gui.KeyF, gui.KeyL)

	return l
}

func (l *AgentPanel) Name() string {
	return AgentPanelName
}

func (l *AgentPanel) queryState() {
	podList := &corev1.PodList{}
	req := l.gui.K8sClient.KubeClient.Discovery().RESTClient().Get().RequestURI("/api/v1/pods?labelSelector=app%3Dloggie&limit=1000")
	err := req.Do(context.Background()).Into(podList)
	if err != nil {
		log.Error("list all loggie pods err: %+v", err)
		return
	}

	l.gui.SetLoggieAgentList(podList)

	selector := labels.FormatLabels(map[string]string{
		"app": "loggie",
	})
	podMetricsList, err := l.gui.K8sClient.MetricsClient.MetricsV1beta1().PodMetricses("").List(context.Background(), metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		log.Warn("list all loggie pods metrics err: %+v", err)
	}

	podMetricsMap := make(map[string]PodMetric)
	if podMetricsList != nil {
		for _, item := range podMetricsList.Items {
			key := metricsKey(item.Namespace, item.Name)
			if len(item.Containers) == 0 {
				continue
			}
			val := PodMetric{
				CPU:    fmt.Sprintf("%vm", item.Containers[0].Usage.Cpu().MilliValue()),
				Memory: fmt.Sprintf("%vMi", item.Containers[0].Usage.Memory().Value()/(1024*1024)),
			}
			podMetricsMap[key] = val
		}
	}

	var states []AgentState
	for _, item := range podList.Items {
		if l.filterWord != "" && !strings.Contains(item.Name, l.filterWord) {
			continue
		}
		key := metricsKey(item.Namespace, item.Name)

		var cpu, mem string
		if m, ok := podMetricsMap[key]; ok {
			cpu = m.CPU
			mem = m.Memory
		}

		state := AgentState{
			PodName:   item.Name,
			Namespace: item.Namespace,
			Status:    string(item.Status.Phase),
			Created:   item.CreationTimestamp,
			CPU:       cpu,
			Memory:    mem,
			IP:        item.Status.PodIP,
			Node:      item.Spec.NodeName,
		}
		if len(item.Status.ContainerStatuses) > 0 {
			state.Restarts = item.Status.ContainerStatuses[0].RestartCount
		}

		states = append(states, state)
	}
	l.states = states

	count := len(podList.Items)
	l.SetTitle(fmt.Sprintf(" Loggie Pods | All: %s%d ", gui.ColorTextPurple, count))
	l.gui.GlobalStat.LoggiePodCount = count
}

func metricsKey(namespace string, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

func (l *AgentPanel) SetData() {
	l.queryState()
	renderTable(l.Table, l.states)
}

func renderTable(t *tview.Table, states []AgentState) {
	table := t.Clear()

	headers := []string{
		"Namespace/PodName",
		"Status",
		"Node",
		"Restarts (Created)",
		"CPU/Memory",
	}

	for i, header := range headers {
		table.SetCell(0, i, &tview.TableCell{
			Text:            header,
			NotSelectable:   true,
			Align:           tview.AlignLeft,
			Color:           tcell.ColorWhite,
			BackgroundColor: tcell.ColorDefault,
			Attributes:      tcell.AttrBold,
		})
	}

	for i, state := range states {
		table.SetCell(i+1, 0, tview.NewTableCell(fmt.Sprintf("%s/%s", state.Namespace, state.PodName)).
			SetMaxWidth(0).
			SetExpansion(1))

		status := tview.NewTableCell(state.Status).SetMaxWidth(0).SetExpansion(1)
		if state.Status != "Running" {
			status.SetTextColor(gui.ColorRed)
		}
		table.SetCell(i+1, 1, status)

		table.SetCell(i+1, 2, tview.NewTableCell(state.Node).
			SetMaxWidth(0).
			SetExpansion(1))

		table.SetCell(i+1, 3, tview.NewTableCell(fmt.Sprintf("%d (%s ago)", state.Restarts, time.TranslateTimestampSince(state.Created))).
			SetMaxWidth(0).
			SetExpansion(1))

		table.SetCell(i+1, 4, tview.NewTableCell(fmt.Sprintf("%s/%s", state.CPU, state.Memory)).
			SetMaxWidth(0).
			SetExpansion(1))
	}

}

func (l *AgentPanel) UpdateData(g *gui.Gui) {
	go g.App.QueueUpdateDraw(func() {
		l.SetData()
	})
}

func (l *AgentPanel) Focus() {
	gui.TableFocus(l.Table, l.gui, cellColor)
}

func (l *AgentPanel) UnFocus() {
	gui.TableUnFocus(l.Table)
}

func (l *AgentPanel) SetFilterWord(word string) {
	l.filterWord = word
}

func (l *AgentPanel) SetKeybinding(g *gui.Gui) {
	l.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		g.SetGlobalKeybinding(event)
		switch event.Key() {
		case tcell.KeyEnter:
			switchToLoggieDetail(l.Table, l.gui, l.states, gui.MainPage)
		}

		switch event.Rune() {
		case 'l':
			logs(l.Table, l.gui, l.states, gui.MainPage)

		case 'o':
			l.showList()
		}

		return event
	})
}

func switchToLoggieDetail(table *tview.Table, g *gui.Gui, states []AgentState, returnPage string) {
	if len(states) == 0 {
		return
	}

	selectedRow, _ := table.GetSelection()
	state := states[selectedRow-1]

	text := NewLoggiePipelineDetailPanel(g, "", "", state.IP)
	logStatus := NewLogStatusPanel(g, "", "", state.IP)

	grid := tview.NewGrid().
		SetRows(0, 0, 3).
		AddItem(text.TextView, 0, 0, 1, 1, 0, 0, true).
		AddItem(logStatus.TreeView, 1, 0, 1, 1, 0, 0, true).
		AddItem(g.Nav.TextView, 2, 0, 1, 1, 0, 0, false)

	grid.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || event.Rune() == 'q' {
			g.CloseAndSwitchPanel(gui.LoggieDetailPage, returnPage, AgentPanelName)
		}
		return event
	})

	g.AddPanels(gui.LoggieDetailPage, text, logStatus)
	g.SetCurrentPage(gui.LoggieDetailPage)
	g.Pages.AddAndSwitchToPage(gui.LoggieDetailPage, grid, true)
	g.SwitchPanel(PipelineDetailPanelName)
}

func logs(table *tview.Table, g *gui.Gui, states []AgentState, returnPage string) {
	if len(states) == 0 {
		return
	}

	selectedRow, _ := table.GetSelection()
	state := states[selectedRow-1]

	SwitchLogPanel(g, state.Namespace, state.PodName, returnPage)
}

func SwitchLogPanel(g *gui.Gui, namespace string, podName string, returnPage string) {
	logsPanel := NewLoggieLogPanel(g, namespace, podName)

	grid := tview.NewGrid().
		AddItem(logsPanel.TextView, 0, 0, 1, 1, 0, 0, true)

	grid.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || event.Rune() == 'q' {
			g.CloseAndSwitchPanel(gui.LoggieLogsPage, returnPage, LogPanelName)
		}
		return event
	})

	g.SetCurrentPage(gui.LoggieLogsPage)
	g.Pages.AddAndSwitchToPage(gui.LoggieLogsPage, grid, true)
}

func (l *AgentPanel) showList() {
	listPanel := NewLoggieListPanel(l.gui, l.states)

	grid := tview.NewGrid().
		AddItem(listPanel.Table, 0, 0, 1, 1, 0, 0, true)

	grid.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || event.Rune() == 'q' {
			l.gui.CloseAndSwitchPanel(gui.LoggieListPage, gui.MainPage, AgentPanelName)
		}
		return event
	})

	l.gui.AddPanels(gui.LoggieListPage, listPanel)
	l.gui.SetCurrentPage(gui.LoggieListPage)
	l.gui.Pages.AddAndSwitchToPage(gui.LoggieListPage, grid, true)
}
