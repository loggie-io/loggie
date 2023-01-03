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

package logconfig

import (
	"context"
	"fmt"
	"github.com/gdamore/tcell/v2"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/content"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/gui"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/loggie"
	"github.com/rivo/tview"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
	"strings"
)

const (
	YamlPanelName        = "LogConfigYaml"
	SelectedPodPanelName = "LogConfigSelectedPods"
	EventsPanelName      = "LogConfigEventsPanel"
)

type YamlPanel struct {
	gui *gui.Gui
	*tview.TextView

	logConfigName string
	namespace     string

	text   string
	filter string
}

func newLogConfigYamlPanel(g *gui.Gui, name string, namespace string) *YamlPanel {
	p := &YamlPanel{
		gui:      g,
		TextView: tview.NewTextView().SetDynamicColors(true),
	}

	p.TextView.SetTitle(" LogConfig Yaml ").
		SetBorder(true).
		SetTitleAlign(tview.AlignCenter)

	p.logConfigName = name
	p.namespace = namespace
	p.SetData()
	p.SetKeybinding(g)
	g.Nav.AddKeyBindingsNavWithKey(YamlPanelName, "", gui.KeyTab, gui.KeyO, gui.KeyQ, gui.KeyF, gui.KeyCtrlF, gui.KeyCtrlB, gui.KeyG, gui.KeyShiftG)

	return p
}

func (p *YamlPanel) Name() string {
	return YamlPanelName
}

func (p *YamlPanel) SetData() {
	result, err := p.gui.K8sClient.LgcClient.LoggieV1beta1().LogConfigs(p.namespace).Get(context.Background(), p.logConfigName, metav1.GetOptions{})
	if err != nil {
		p.SetText(err.Error()).SetTextColor(gui.ColorRed)
		return
	}

	result.ManagedFields = nil
	out, err := yaml.Marshal(result)
	if err != nil {
		p.SetText(err.Error()).SetTextColor(gui.ColorRed)
		return
	}

	highlighted := content.YamlHighlight(string(out), p.filter)
	p.SetText(highlighted)
	p.text = highlighted
}

func (p *YamlPanel) UpdateData(g *gui.Gui) {
	go g.App.QueueUpdateDraw(func() {
		p.SetData()
	})
}

func (p *YamlPanel) Focus() {
	gui.TextViewFocus(p.TextView, p.gui, gui.ColorTeal)
}

func (p *YamlPanel) UnFocus() {
	gui.TextViewUnFocus(p.TextView)
}

func (p *YamlPanel) SetFilterWord(word string) {
	p.filter = word
}

func (p *YamlPanel) SetKeybinding(g *gui.Gui) {
	p.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		g.SetGlobalKeybinding(event)
		switch event.Key() {
		case tcell.KeyEnter:
			p.switchToYamlDetail()
		}

		switch event.Rune() {
		case 'o':
			p.switchToYamlDetail()
		}

		return event
	})
}

func (p *YamlPanel) switchToYamlDetail() {
	text := content.New(p.gui, p.text)

	grid := tview.NewGrid().
		SetRows(0, 3).
		AddItem(text.TextView, 0, 0, 1, 1, 0, 0, true).
		AddItem(p.gui.Nav.TextView, 1, 0, 1, 1, 0, 0, false)

	grid.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || event.Rune() == 'q' {
			p.gui.CloseAndSwitchPanel(gui.YamlDetailPage, gui.LogConfigDetailPage, PanelName)
		}
		return event
	})

	p.gui.AddPanels(gui.YamlDetailPage, text)
	p.gui.SetCurrentPage(gui.YamlDetailPage)
	p.gui.Pages.AddAndSwitchToPage(gui.YamlDetailPage, grid, true)
}

// ---

type PodSelectorPanel struct {
	gui *gui.Gui
	*tview.Table

	podSelectorMap map[string]string
	logConfigName  string
	namespace      string

	states     []PodSelectorStates
	filterWord string
}

type PodSelectorStates struct {
	PodName         string
	PodStatus       string
	LoggiePodName   string
	LoggieNamespace string
	LoggiePodIP     string
	LoggiePodStatus string
	Node            string
}

func newLogConfigPodSelectorPanel(g *gui.Gui, podSelectorMap map[string]string, logConfigName string, namespace string) *PodSelectorPanel {
	l := &PodSelectorPanel{
		gui:            g,
		Table:          tview.NewTable().SetSelectable(true, false).Select(0, 0).SetFixed(1, 1),
		podSelectorMap: podSelectorMap,
		logConfigName:  logConfigName,
		namespace:      namespace,
	}

	l.SetTitleAlign(tview.AlignCenter)
	l.SetBorder(true)
	l.SetSelectedStyle(gui.NewTableSelectedStyle(gui.ColorYellow))
	l.SetData()
	l.SetKeybinding(g)
	g.Nav.AddKeyBindingsNavWithKey(SelectedPodPanelName, "", gui.KeyTab, gui.KeyEnter, gui.KeyF, gui.KeyR, gui.KeyQ, gui.KeyL)

	return l
}

func (l *PodSelectorPanel) Name() string {
	return SelectedPodPanelName
}

func (l *PodSelectorPanel) queryState() {
	selector := make(map[string]string)
	for k, v := range l.podSelectorMap {
		selector[k] = v
	}

	sel, err := helper.Selector(selector)
	if err != nil {
		return
	}
	podList, err := l.gui.K8sClient.KubeClient.CoreV1().Pods(l.namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: sel.String(),
	})
	if err != nil {
		return
	}

	var states []PodSelectorStates
	for _, pod := range podList.Items {
		if !strings.Contains(pod.Name, l.filterWord) {
			continue
		}

		var loggieName string
		var loggieNamespace string
		var loggieStatus string
		var loggiePodIP string
		loggiePod := l.gui.GetLoggieAgentPod(pod.Spec.NodeName)
		if loggiePod == nil {
			loggieName = "NotFound"
			loggiePodIP = "None"
		} else {
			loggieName = loggiePod.Name
			loggieNamespace = loggiePod.Namespace
			loggieStatus = string(loggiePod.Status.Phase)
			loggiePodIP = loggiePod.Status.PodIP
		}

		stat := PodSelectorStates{
			PodName:         pod.Name,
			PodStatus:       string(pod.Status.Phase),
			LoggiePodName:   loggieName,
			LoggieNamespace: loggieNamespace,
			LoggiePodIP:     loggiePodIP,
			LoggiePodStatus: loggieStatus,
			Node:            pod.Spec.NodeName,
		}
		states = append(states, stat)
	}

	l.states = states
}

func (l *PodSelectorPanel) SetData() {
	l.queryState()

	table := l.Clear()

	l.SetTitle(fmt.Sprintf(" Selected Pods | All: %s%d ", gui.ColorTextPurple, len(l.states)))

	headers := []string{
		"PodName",
		"PodStatus",
		"Loggie.PodName",
		"Loggie.PodStatus",
		"Node",
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

	for i, state := range l.states {
		table.SetCell(i+1, 0, tview.NewTableCell(state.PodName).
			SetMaxWidth(0).
			SetExpansion(1))

		table.SetCell(i+1, 1, tview.NewTableCell(state.PodStatus).
			SetMaxWidth(0).
			SetExpansion(1))

		table.SetCell(i+1, 2, tview.NewTableCell(state.LoggiePodName).
			SetMaxWidth(0).
			SetExpansion(1))

		table.SetCell(i+1, 3, tview.NewTableCell(state.LoggiePodStatus).
			SetMaxWidth(0).
			SetExpansion(1))

		table.SetCell(i+1, 4, tview.NewTableCell(state.Node).
			SetMaxWidth(0).
			SetExpansion(1))
	}

}

func (l *PodSelectorPanel) UpdateData(g *gui.Gui) {
	go g.App.QueueUpdateDraw(func() {
		l.SetData()
	})
}

func (l *PodSelectorPanel) Focus() {
	gui.TableFocus(l.Table, l.gui, gui.ColorYellow)
}

func (l *PodSelectorPanel) UnFocus() {
	gui.TableUnFocus(l.Table)
}

func (l *PodSelectorPanel) SetFilterWord(word string) {
	l.filterWord = word
}

func (l *PodSelectorPanel) SetKeybinding(g *gui.Gui) {
	l.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		g.SetGlobalKeybinding(event)
		switch event.Key() {
		case tcell.KeyEnter:
			l.switchToLoggieDetail(g)
		}

		switch event.Rune() {
		case 'l':
			if len(l.states) == 0 {
				break
			}
			selectedRow, _ := l.Table.GetSelection()
			state := l.states[selectedRow-1]
			loggie.SwitchLogPanel(l.gui, state.LoggieNamespace, state.LoggiePodName, gui.LogConfigDetailPage)
		}

		return event
	})
}

func (l *PodSelectorPanel) switchToLoggieDetail(g *gui.Gui) {
	if len(l.states) == 0 {
		return
	}

	selectedRow, _ := l.GetSelection()
	state := l.states[selectedRow-1]

	text := loggie.NewLoggiePipelineDetailPanel(g, l.logConfigName, l.namespace, state.LoggiePodIP)
	logStatus := loggie.NewLogStatusPanel(g, l.logConfigName, l.namespace, state.LoggiePodIP)

	grid := tview.NewGrid().
		SetRows(0, 0, 3).
		AddItem(text.TextView, 0, 0, 1, 1, 0, 0, true).
		AddItem(logStatus.TreeView, 1, 0, 1, 1, 0, 0, true).
		AddItem(g.Nav.TextView, 2, 0, 1, 1, 0, 0, false)

	grid.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || event.Rune() == 'q' {
			g.CloseAndSwitchPanel(gui.LoggieDetailPage, gui.LogConfigDetailPage, SelectedPodPanelName)
		}
		return event
	})

	l.gui.AddPanels(gui.LoggieDetailPage, text, logStatus)
	l.gui.SetCurrentPage(gui.LoggieDetailPage)
	g.Pages.AddAndSwitchToPage(gui.LoggieDetailPage, grid, true)
	l.gui.SwitchPanel(loggie.PipelineDetailPanelName)
}

//---

type EventsPanel struct {
	*tview.TextView
	gui *gui.Gui

	logConfigName string
	namespace     string

	filterWord string
}

func newLogConfigEventsPanel(g *gui.Gui, name string, namespace string) *EventsPanel {
	l := &EventsPanel{
		gui:      g,
		TextView: tview.NewTextView().SetDynamicColors(true),
	}

	l.logConfigName = name
	l.namespace = namespace

	l.SetTitle(" Events ").SetTitleAlign(tview.AlignCenter)
	l.SetBorder(true)
	l.SetData()
	l.SetKeybinding(g)

	return l
}

func (l *EventsPanel) Name() string {
	return EventsPanelName
}

func (l *EventsPanel) SetData() {
	events, err := l.gui.K8sClient.KubeClient.CoreV1().Events(l.namespace).List(context.Background(), metav1.ListOptions{
		FieldSelector: fmt.Sprintf("involvedObject.kind=LogConfig,involvedObject.name=%s", l.logConfigName),
	})
	if err != nil {
		l.TextView.SetText(err.Error())
		return
	}

	if len(events.Items) == 0 {
		l.TextView.SetText("  [red]Events None")
		return
	}

	var out strings.Builder
	out.WriteString("  Type  |  Reason  |  Message  |  From  |  LastTimestamp\n")
	out.WriteString("  ----  |  ------  |  -------  |  ----  |  -------------\n")
	for _, e := range events.Items {
		out.WriteString("  [gray]" + e.Type + " | ")
		out.WriteString(e.Reason + " | ")
		out.WriteString(e.Message + " | ")
		out.WriteString(e.Source.Component + " | ")
		out.WriteString(e.LastTimestamp.Format(gui.TsLayout) + "\n")
	}

	l.TextView.SetText(out.String())
}

func (l *EventsPanel) UpdateData(g *gui.Gui) {
	go g.App.QueueUpdateDraw(func() {
		l.SetData()
	})
}

func (l *EventsPanel) Focus() {
	gui.TextViewFocus(l.TextView, l.gui, gui.ColorWhite)
}

func (l *EventsPanel) UnFocus() {
	gui.TextViewUnFocus(l.TextView)
}

func (l *EventsPanel) SetFilterWord(word string) {
	l.filterWord = word
}

func (l *EventsPanel) SetKeybinding(g *gui.Gui) {
	l.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		g.SetGlobalKeybinding(event)
		switch event.Key() {
		case tcell.KeyEnter:
		}

		return event
	})
}
