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

package content

import (
	"fmt"
	"github.com/gdamore/tcell/v2"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/gui"
	"github.com/rivo/tview"
	"strings"
)

const (
	PanelName = "AllContent"
)

type Panel struct {
	gui *gui.Gui
	*tview.TextView

	content string
	filter  string
}

func New(g *gui.Gui, content string) *Panel {
	p := &Panel{
		gui:      g,
		content:  content,
		TextView: tview.NewTextView().SetDynamicColors(true),
	}

	p.TextView.SetTitle(" Content ").SetBorder(true).SetTitleAlign(tview.AlignCenter).SetBorderColor(gui.ColorTeal)
	p.SetData()
	p.SetKeybinding(g)
	g.Nav.AddKeyBindingsNavWithKey(PanelName, "", gui.KeyQ, gui.KeyCtrlF, gui.KeyCtrlB, gui.KeyG, gui.KeyShiftG, gui.KeyR)

	return p
}

func (p *Panel) Name() string {
	return PanelName
}

func (p *Panel) SetData() {
	if p.filter == "" {
		p.SetText(p.content)
		return
	}

	str := p.content
	if strings.Contains(str, p.filter) {
		str = strings.ReplaceAll(str, p.filter, fmt.Sprintf("[red]%s[white]", p.filter))
	}

	p.SetText(str)
}

func (p *Panel) UpdateData(g *gui.Gui) {
	go g.App.QueueUpdateDraw(func() {
		p.SetData()
	})
}

func (p *Panel) Focus() {
	p.gui.App.SetFocus(p.TextView)
}

func (p *Panel) UnFocus() {
}

func (p *Panel) SetFilterWord(word string) {
	p.filter = word
}

func (p *Panel) SetKeybinding(g *gui.Gui) {
	p.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		g.SetGlobalKeybinding(event)
		return event
	})
}
