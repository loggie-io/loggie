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
	"github.com/loggie-io/loggie/pkg/core/log"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/gui"
	"github.com/loggie-io/loggie/pkg/util/time"
	"github.com/rivo/tview"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"strings"
)

const PanelName = "LogConfig"

var (
	cellColor = gui.ColorGreen
)

type Panel struct {
	gui *gui.Gui
	*tview.Table
	filterWord string

	states []State
}

type State struct {
	Name           string
	Namespace      string
	PodSelectorMap map[string]string
	PodSelector    string
	Cluster        string
	Created        metav1.Time
}

func NewLogConfigPanel(g *gui.Gui) *Panel {
	l := &Panel{
		gui:   g,
		Table: tview.NewTable().SetSelectable(true, false).Select(0, 0).SetFixed(1, 1),
	}

	l.SetTitleAlign(tview.AlignCenter)
	l.SetSelectedStyle(gui.NewTableSelectedStyle(cellColor))
	l.SetBorder(true)
	l.SetData()
	l.SetKeybinding(g)
	g.Nav.AddKeyBindingsNavWithKey(PanelName, "", gui.KeyTab, gui.KeyEnter, gui.KeyO, gui.KeyQ, gui.KeyF, gui.KeyR)

	return l
}

func (l *Panel) Name() string {
	return PanelName
}

func (l *Panel) queryState() {
	lgcList := &logconfigv1beta1.LogConfigList{}
	req := l.gui.K8sClient.KubeClient.Discovery().RESTClient().Get().RequestURI("/apis/loggie.io/v1beta1/logconfigs?limit=500")
	err := req.Do(context.Background()).Into(lgcList)
	if err != nil {
		log.Error("list all logconfigs err: %+v", err)
		return
	}

	var states []State
	for _, item := range lgcList.Items {
		if l.filterWord != "" && !strings.Contains(item.Name, l.filterWord) {
			continue
		}

		state := State{
			Name:           item.Name,
			Namespace:      item.Namespace,
			PodSelector:    labels.FormatLabels(item.Spec.Selector.LabelSelector),
			PodSelectorMap: item.Spec.Selector.LabelSelector,
			Cluster:        item.Spec.Selector.Cluster,
			Created:        item.CreationTimestamp,
		}

		states = append(states, state)
	}
	l.states = states

	count := len(lgcList.Items)
	l.SetTitle(fmt.Sprintf(" LogConfig | All: %s%d ", gui.ColorTextPurple, count))
	l.gui.GlobalStat.LogConfigCount = count
}

func (l *Panel) SetData() {
	l.queryState()

	renderTable(l.Table, l.states)
}

func renderTable(t *tview.Table, states []State) {
	table := t.Clear()

	headers := []string{
		"Namespace",
		"Name",
		"LabelSelector",
		"Created",
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
		table.SetCell(i+1, 0, tview.NewTableCell(state.Namespace).
			SetMaxWidth(0).
			SetExpansion(1))

		table.SetCell(i+1, 1, tview.NewTableCell(state.Name).
			SetMaxWidth(0).
			SetExpansion(1))

		table.SetCell(i+1, 2, tview.NewTableCell(state.PodSelector).
			SetMaxWidth(0).
			SetExpansion(1))

		table.SetCell(i+1, 3, tview.NewTableCell(fmt.Sprintf("%s ago", time.TranslateTimestampSince(state.Created))).
			SetMaxWidth(0).
			SetExpansion(1))
	}

}

func (l *Panel) UpdateData(g *gui.Gui) {
	go g.App.QueueUpdateDraw(func() {
		l.SetData()
	})
}

func (l *Panel) Focus() {
	gui.TableFocus(l.Table, l.gui, cellColor)
}

func (l *Panel) UnFocus() {
	gui.TableUnFocus(l.Table)
}

func (l *Panel) SetFilterWord(word string) {
	l.filterWord = word
}

func (l *Panel) SetKeybinding(g *gui.Gui) {
	l.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		g.SetGlobalKeybinding(event)
		switch event.Key() {
		case tcell.KeyEnter:
			l.switchToLogConfigDetails()
		}

		switch event.Rune() {
		case 'o':
			l.showList()
		}

		return event
	})
}

func (l *Panel) switchToLogConfigDetails() {
	switchToLogConfigDetails(l.Table, l.gui, l.states, gui.MainPage)
}

func switchToLogConfigDetails(table *tview.Table, g *gui.Gui, states []State, returnPage string) {
	if len(states) == 0 {
		return
	}

	selectedRow, _ := table.GetSelection()
	state := states[selectedRow-1]
	name := state.Name
	namespace := state.Namespace

	text := newLogConfigYamlPanel(g, name, namespace)
	selectedPods := newLogConfigPodSelectorPanel(g, state.PodSelectorMap, name, namespace)
	events := newLogConfigEventsPanel(g, name, namespace)

	grid := tview.NewGrid().
		SetRows(16, 0, 0, 3).
		AddItem(text.TextView, 0, 0, 1, 1, 0, 0, true).
		AddItem(selectedPods.Table, 1, 0, 1, 1, 0, 0, true).
		AddItem(events.TextView, 2, 0, 1, 1, 0, 0, true).
		AddItem(g.Nav.TextView, 3, 0, 1, 1, 0, 0, false)

	grid.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || event.Rune() == 'q' {
			g.CloseAndSwitchPanel(gui.LogConfigDetailPage, returnPage, PanelName)
		}
		return event
	})

	g.AddPanels(gui.LogConfigDetailPage, text, selectedPods, events)
	g.SetCurrentPage(gui.LogConfigDetailPage)
	g.Pages.AddAndSwitchToPage(gui.LogConfigDetailPage, grid, true)
	g.SwitchPanel(YamlPanelName)
}

func (l *Panel) showList() {
	listPanel := NewLogConfigListPanel(l.gui, l.states)

	grid := tview.NewGrid().
		AddItem(listPanel.Table, 0, 0, 1, 1, 0, 0, true)

	grid.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || event.Rune() == 'q' {
			l.gui.CloseAndSwitchPanel(gui.LogConfigListPage, gui.MainPage, PanelName)
		}
		return event
	})

	l.gui.AddPanels(gui.LogConfigListPage, listPanel)
	l.gui.SetCurrentPage(gui.LogConfigListPage)
	l.gui.Pages.AddAndSwitchToPage(gui.LogConfigListPage, grid, true)
}
