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
	"fmt"
	"github.com/gdamore/tcell/v2"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/gui"
	"github.com/rivo/tview"
	"strings"
)

const ListPanelName = "LogConfigList"

type ListPanel struct {
	gui *gui.Gui
	*tview.Table
	filterWord string

	states []State
}

func NewLogConfigListPanel(g *gui.Gui, state []State) *ListPanel {
	l := &ListPanel{
		gui:    g,
		Table:  tview.NewTable().SetSelectable(true, false).Select(0, 0).SetFixed(1, 1),
		states: state,
	}

	l.SetTitleAlign(tview.AlignCenter).SetTitleColor(cellColor)
	l.SetSelectedStyle(gui.NewTableSelectedStyle(cellColor))
	l.SetBorder(true).SetBorderColor(cellColor)
	l.SetData()
	l.SetKeybinding(g)
	g.Nav.AddKeyBindingsNavWithKey(ListPanelName, "", gui.KeyEnter, gui.KeyQ, gui.KeyF)

	return l
}

func (l *ListPanel) Name() string {
	return ListPanelName
}

func (l *ListPanel) SetData() {
	count := len(l.states)
	l.SetTitle(fmt.Sprintf(" LogConfig | All: %s%d ", gui.ColorTextPurple, count))

	if l.filterWord != "" {
		var filterStat []State
		for _, s := range l.states {
			if !strings.Contains(s.Name, l.filterWord) {
				continue
			}
			filterStat = append(filterStat, s)
		}
		renderTable(l.Table, filterStat)
		return
	}

	renderTable(l.Table, l.states)
}

func (l *ListPanel) UpdateData(g *gui.Gui) {
	go g.App.QueueUpdateDraw(func() {
		l.SetData()
	})
}

func (l *ListPanel) Focus() {
	l.SetSelectable(true, false)
	l.gui.App.SetFocus(l.Table)
}

func (l *ListPanel) UnFocus() {
	l.SetSelectable(false, false)
}

func (l *ListPanel) SetFilterWord(word string) {
	l.filterWord = word
}

func (l *ListPanel) SetKeybinding(g *gui.Gui) {
	l.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		g.SetGlobalKeybinding(event)
		switch event.Key() {
		case tcell.KeyEnter:
			switchToLogConfigDetails(l.Table, l.gui, l.states, gui.LogConfigListPage)
		}

		return event
	})
}
