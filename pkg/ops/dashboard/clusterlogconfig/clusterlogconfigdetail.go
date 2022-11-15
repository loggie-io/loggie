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

package clusterlogconfig

import (
	"context"
	"fmt"
	"github.com/gdamore/tcell/v2"
	"github.com/loggie-io/loggie/pkg/core/log"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/content"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/gui"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/loggie"
	"github.com/rivo/tview"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/yaml"
	"strings"
)

const (
	YamlPanelName           = "ClusterLogConfigYaml"
	SelectedLoggiePanelName = "ClusterLogConfigSelected"
	EventsPanelName         = "ClusterLogConfigEventsPanel"
)

type YamlPanel struct {
	gui *gui.Gui
	*tview.TextView

	logConfigName string

	text   string
	filter string
}

func newClusterLogConfigYamlPanel(g *gui.Gui, name string) *YamlPanel {
	p := &YamlPanel{
		gui:      g,
		TextView: tview.NewTextView().SetDynamicColors(true),
	}

	p.TextView.SetTitle(" ClusterLogConfig Yaml ").SetBorder(true).SetTitleAlign(tview.AlignCenter)

	p.logConfigName = name
	p.SetData()
	p.SetKeybinding(g)
	g.Nav.AddKeyBindingsNavWithKey(YamlPanelName, "", gui.KeyTab, gui.KeyO, gui.KeyQ, gui.KeyF, gui.KeyCtrlF, gui.KeyCtrlB, gui.KeyG, gui.KeyShiftG)

	return p
}

func (p *YamlPanel) Name() string {
	return YamlPanelName
}

func (p *YamlPanel) SetData() {
	result, err := p.gui.K8sClient.LgcClient.LoggieV1beta1().ClusterLogConfigs().Get(context.Background(), p.logConfigName, metav1.GetOptions{})
	if err != nil {
		p.SetText(err.Error()).SetTextColor(tcell.ColorRed)
		return
	}

	result.ManagedFields = nil
	out, err := yaml.Marshal(result)
	if err != nil {
		p.SetText(err.Error()).SetTextColor(tcell.ColorRed)
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
			p.gui.CloseAndSwitchPanel(gui.YamlDetailPage, gui.ClusterLogConfigDetailPage, PanelName)
		}
		return event
	})

	p.gui.AddPanels(gui.YamlDetailPage, text)
	p.gui.SetCurrentPage(gui.YamlDetailPage)
	p.gui.Pages.AddAndSwitchToPage(gui.YamlDetailPage, grid, true)
}

// ---

type SelectorPanel struct {
	gui *gui.Gui
	*tview.Table

	selectType           string
	selector             map[string]string
	clusterLogConfigName string

	typePodStates  []SelectorTypePodStates
	typeNodeStates []SelectorTypeNodeStates
	filterWord     string
}

type SelectorTypePodStates struct {
	PodName         string
	PodStatus       string
	Namespace       string
	LoggiePodName   string
	LoggieNamespace string
	LoggiePodIP     string
	LoggiePodStatus string
	Node            string
}

type SelectorTypeNodeStates struct {
	LoggiePodName   string
	LoggieNamespace string
	LoggiePodIP     string
	LoggiePodStatus string
	Node            string
	NodeStatus      string
}

func newClusterLogConfigPodSelectorPanel(g *gui.Gui, selectType string, selector map[string]string, clusterLogConfigName string) *SelectorPanel {
	l := &SelectorPanel{
		gui:                  g,
		Table:                tview.NewTable().SetSelectable(true, false).Select(0, 0).SetFixed(1, 1),
		selectType:           selectType,
		selector:             selector,
		clusterLogConfigName: clusterLogConfigName,
	}

	l.SetTitle(" Selected Loggie ").SetTitleAlign(tview.AlignCenter)
	l.SetBorder(true)
	l.SetSelectedStyle(gui.NewTableSelectedStyle(gui.ColorYellow))
	l.SetData()
	l.SetKeybinding(g)
	g.Nav.AddKeyBindingsNavWithKey(SelectedLoggiePanelName, "", gui.KeyTab, gui.KeyEnter, gui.KeyF, gui.KeyR, gui.KeyQ, gui.KeyL)

	return l
}

func (l *SelectorPanel) Name() string {
	return SelectedLoggiePanelName
}

func (l *SelectorPanel) queryStateTypePod() error {
	selector := make(map[string]string)
	for k, v := range l.selector {
		selector[k] = v
	}
	sel, err := helper.Selector(selector)
	if err != nil {
		return err
	}
	podList, err := l.gui.K8sClient.KubeClient.CoreV1().Pods(corev1.NamespaceAll).List(context.Background(), metav1.ListOptions{
		LabelSelector: sel.String(),
	})
	if err != nil {
		return err
	}

	var states []SelectorTypePodStates
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

		stat := SelectorTypePodStates{
			PodName:         pod.Name,
			PodStatus:       string(pod.Status.Phase),
			Namespace:       pod.Namespace,
			LoggiePodName:   loggieName,
			LoggieNamespace: loggieNamespace,
			LoggiePodIP:     loggiePodIP,
			LoggiePodStatus: loggieStatus,
			Node:            pod.Spec.NodeName,
		}
		states = append(states, stat)
	}

	l.typePodStates = states
	return nil
}

func (l *SelectorPanel) queryStateTypeNode() error {
	selector := make(map[string]string)
	for k, v := range l.selector {
		selector[k] = v
	}

	nodeList, err := l.gui.K8sClient.KubeClient.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{
		LabelSelector: labels.FormatLabels(selector),
	})
	if err != nil {
		return err
	}

	var states []SelectorTypeNodeStates
	for _, node := range nodeList.Items {
		if !strings.Contains(node.Name, l.filterWord) {
			continue
		}

		var loggieName string
		var loggieNamespace string
		var loggieStatus string
		var loggiePodIP string
		loggiePod := l.gui.GetLoggieAgentPod(node.Name)
		if loggiePod == nil {
			loggieName = "NotFound"
			loggiePodIP = "None"
		} else {
			loggieName = loggiePod.Name
			loggieNamespace = loggiePod.Namespace
			loggieStatus = string(loggiePod.Status.Phase)
			loggiePodIP = loggiePod.Status.PodIP
		}

		stat := SelectorTypeNodeStates{
			LoggiePodName:   loggieName,
			LoggieNamespace: loggieNamespace,
			LoggiePodIP:     loggiePodIP,
			LoggiePodStatus: loggieStatus,
			Node:            node.Name,
		}

		states = append(states, stat)
	}

	l.typeNodeStates = states
	return nil
}

func (l *SelectorPanel) setDataTypePod() {
	table := l.Clear()
	l.SetTitle(fmt.Sprintf(" Selected Pods | All: %s%d ", gui.ColorTextPurple, len(l.typePodStates)))

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

	for i, state := range l.typePodStates {
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

func (l *SelectorPanel) setDataTypeNode() {
	table := l.Clear()
	l.SetTitle(fmt.Sprintf(" Selected Nodes | All: %s%d ", gui.ColorTextPurple, len(l.typeNodeStates)))

	headers := []string{
		"NodeName",
		"Loggie.PodName",
		"Loggie.PodStatus",
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

	for i, state := range l.typeNodeStates {
		table.SetCell(i+1, 0, tview.NewTableCell(state.Node).
			SetMaxWidth(0).
			SetExpansion(1))

		table.SetCell(i+1, 1, tview.NewTableCell(state.LoggiePodName).
			SetMaxWidth(0).
			SetExpansion(1))

		table.SetCell(i+1, 2, tview.NewTableCell(state.LoggiePodStatus).
			SetMaxWidth(0).
			SetExpansion(1))
	}
}

func (l *SelectorPanel) SetData() {
	if l.selectType == logconfigv1beta1.SelectorTypePod {
		err := l.queryStateTypePod()
		if err != nil {
			// TODO show error
			return
		}

		l.setDataTypePod()

	} else if l.selectType == logconfigv1beta1.SelectorTypeNode {
		err := l.queryStateTypeNode()
		if err != nil {
			// TODO show error
			return
		}

		l.setDataTypeNode()
	}
}

func (l *SelectorPanel) UpdateData(g *gui.Gui) {
	go g.App.QueueUpdateDraw(func() {
		l.SetData()
	})
}

func (l *SelectorPanel) Focus() {
	gui.TableFocus(l.Table, l.gui, gui.ColorYellow)
}

func (l *SelectorPanel) UnFocus() {
	gui.TableUnFocus(l.Table)
}

func (l *SelectorPanel) SetFilterWord(word string) {
	l.filterWord = word
}

func (l *SelectorPanel) SetKeybinding(g *gui.Gui) {
	l.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		g.SetGlobalKeybinding(event)
		switch event.Key() {
		case tcell.KeyEnter:
			l.switchToLoggieDetail(g)
		}

		switch event.Rune() {
		case 'l':
			l.logs()
		}

		return event
	})
}

func (l *SelectorPanel) logs() {
	selectedRow, _ := l.Table.GetSelection()

	if l.selectType == logconfigv1beta1.SelectorTypePod {
		if len(l.typePodStates) == 0 {
			return
		}
		state := l.typePodStates[selectedRow-1]
		loggie.SwitchLogPanel(l.gui, state.LoggieNamespace, state.LoggiePodName, gui.ClusterLogConfigDetailPage)

	} else if l.selectType == logconfigv1beta1.SelectorTypeNode {
		if len(l.typeNodeStates) == 0 {
			return
		}
		state := l.typeNodeStates[selectedRow-1]
		loggie.SwitchLogPanel(l.gui, state.LoggieNamespace, state.LoggiePodName, gui.ClusterLogConfigDetailPage)
	}
}

func (l *SelectorPanel) switchToLoggieDetail(g *gui.Gui) {
	if len(l.typePodStates) == 0 && len(l.typeNodeStates) == 0 {
		return
	}

	selectedRow, _ := l.GetSelection()
	var loggieIP string
	if l.selectType == logconfigv1beta1.SelectorTypePod {
		loggieIP = l.typePodStates[selectedRow-1].LoggiePodIP
	} else if l.selectType == logconfigv1beta1.SelectorTypeNode {
		loggieIP = l.typeNodeStates[selectedRow-1].LoggiePodIP
	} else {
		// TODO support type cluster
		log.Panic("type cluster not supported")
	}

	text := loggie.NewLoggiePipelineDetailPanel(g, l.clusterLogConfigName, "", loggieIP)
	logStatus := loggie.NewLogStatusPanel(g, l.clusterLogConfigName, "", loggieIP)

	grid := tview.NewGrid().
		SetRows(0, 0, 3).
		AddItem(text.TextView, 0, 0, 1, 1, 0, 0, true).
		AddItem(logStatus.TreeView, 1, 0, 1, 1, 0, 0, true).
		AddItem(g.Nav.TextView, 2, 0, 1, 1, 0, 0, false)

	grid.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || event.Rune() == 'q' {
			g.CloseAndSwitchPanel(gui.LoggieDetailPage, gui.ClusterLogConfigDetailPage, SelectedLoggiePanelName)
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

func newClusterLogConfigEventsPanel(g *gui.Gui, name string, namespace string) *EventsPanel {
	l := &EventsPanel{
		gui:      g,
		TextView: tview.NewTextView().SetDynamicColors(true),
	}

	l.logConfigName = name
	l.namespace = namespace

	l.SetTitle(" Events ").SetTitleAlign(tview.AlignCenter)
	l.SetBorder(true).SetBorderColor(tcell.ColorLightPink)
	l.SetData()
	l.SetKeybinding(g)

	return l
}

func (l *EventsPanel) Name() string {
	return EventsPanelName
}

func (l *EventsPanel) queryState() {
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
