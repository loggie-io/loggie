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

package gui

import (
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func (g *Gui) SetGlobalKeybinding(event *tcell.EventKey) {
	key := event.Rune()
	switch key {
	case 'w':
		g.PrevPanel()
	case 's':
		g.NextPanel()
	case 'q':
		g.Stop()
	case 'f':
		g.filter()
	case 'r':
		g.refresh()
	}

	switch event.Key() {
	case tcell.KeyTab:
		g.NextPanel()
	case tcell.KeyBacktab:
		g.PrevPanel()
	case tcell.KeyRight:
		g.NextPanel()
	case tcell.KeyLeft:
		g.PrevPanel()
	}
}

func (g *Gui) filter() {
	page := g.panels.currentPage
	pagePanel, ok := g.panels.panel[page]
	if !ok {
		return
	}

	currentPanel := pagePanel[g.panels.currentPanel]
	currentPanel.SetFilterWord("")
	currentPanel.UpdateData(g)

	viewName := "filter"
	searchInput := tview.NewInputField().SetLabel("Name")
	searchInput.SetLabelWidth(6)
	searchInput.SetTitle(" filter ")
	searchInput.SetTitleAlign(tview.AlignCenter)
	searchInput.SetBorder(true)

	closeSearchInput := func() {
		g.CloseAndSwitchPanel(viewName, g.panels.currentPage, pagePanel[g.panels.currentPanel].Name())
	}

	searchInput.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			closeSearchInput()
		}
	})

	searchInput.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			closeSearchInput()
		}
		return event
	})

	searchInput.SetChangedFunc(func(text string) {
		currentPanel.SetFilterWord(text)
		currentPanel.UpdateData(g)
	})

	g.Pages.AddAndSwitchToPage(viewName, g.modal(searchInput, 80, 3), true).ShowPage(g.panels.currentPage)
}
func (g *Gui) refresh() {
	page := g.panels.currentPage
	pagePanel, ok := g.panels.panel[page]
	if !ok {
		return
	}

	currentPanel := pagePanel[g.panels.currentPanel]
	currentPanel.SetFilterWord("")
	currentPanel.UpdateData(g)
}
