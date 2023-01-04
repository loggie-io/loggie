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
	"fmt"
	"github.com/gdamore/tcell/v2"
	jsoniter "github.com/json-iterator/go"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/content"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/gui"
	"github.com/loggie-io/loggie/pkg/ops/helper"
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/rivo/tview"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	PipelineDetailPanelName  = "LoggiePipelineDetail"
	LogStatusDetailPanelName = "LogStatusDetail"
)

type PipelineDetailPanel struct {
	*tview.TextView
	gui *gui.Gui

	logConfigName      string
	logConfigNamespace string
	loggieIP           string

	filter string
}

func NewLoggiePipelineDetailPanel(g *gui.Gui, logConfigName string, logConfigNamespace string, podIP string) *PipelineDetailPanel {
	p := &PipelineDetailPanel{
		gui:      g,
		TextView: tview.NewTextView().SetDynamicColors(true),
	}

	p.TextView.SetTitle(" Pipeline Details ").SetBorder(true).SetTitleAlign(tview.AlignCenter)

	p.logConfigName = logConfigName
	p.logConfigNamespace = logConfigNamespace
	p.loggieIP = podIP
	p.SetData()
	p.SetKeybinding(g)

	g.Nav.AddKeyBindingsNavWithKey(PipelineDetailPanelName, "", gui.KeyTab, gui.KeyO, gui.KeyF, gui.KeyCtrlB, gui.KeyCtrlF, gui.KeyQ)

	return p
}

func (p *PipelineDetailPanel) Name() string {
	return PipelineDetailPanelName
}

func (p *PipelineDetailPanel) SetData() {
	var pipeline string
	if p.logConfigNamespace == "" {
		pipeline = p.logConfigName
	} else {
		pipeline = fmt.Sprintf("%s/%s", p.logConfigNamespace, p.logConfigName)
	}

	u := fmt.Sprintf("http://%s:%d/api/v1/help?detail=pipeline&pipeline=%s", p.loggieIP, p.gui.Config.LoggiePort, pipeline)
	cli := http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := cli.Get(u)
	if err != nil {
		p.SetText(err.Error()).SetTextColor(tcell.ColorRed)
		return
	}
	defer resp.Body.Close()

	out, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		p.SetText(err.Error()).SetTextColor(tcell.ColorRed)
		return
	}

	highlighted := content.YamlHighlight(string(out), p.filter)
	p.SetText(highlighted)
	p.filter = highlighted
}

func (p *PipelineDetailPanel) UpdateData(g *gui.Gui) {
	go g.App.QueueUpdateDraw(func() {
		p.SetData()
	})
}

func (p *PipelineDetailPanel) Focus() {
	gui.TextViewFocus(p.TextView, p.gui, gui.ColorTeal)
}

func (p *PipelineDetailPanel) UnFocus() {
	gui.TextViewUnFocus(p.TextView)
}

func (p *PipelineDetailPanel) SetFilterWord(word string) {
	p.filter = word
}

func (p *PipelineDetailPanel) SetKeybinding(g *gui.Gui) {
	p.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		g.SetGlobalKeybinding(event)
		switch event.Key() {
		case tcell.KeyEnter:
			p.switchToContent()
		}

		switch event.Rune() {
		case 'o':
			p.switchToContent()
		}

		return event
	})
}

func (p *PipelineDetailPanel) switchToContent() {
	text := content.New(p.gui, p.filter)

	grid := tview.NewGrid().
		SetRows(0, 3).
		AddItem(text.TextView, 0, 0, 1, 1, 0, 0, true).
		AddItem(p.gui.Nav.TextView, 1, 0, 1, 1, 0, 0, false)

	grid.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || event.Rune() == 'q' {
			p.gui.CloseAndSwitchPanel(gui.YamlDetailPage, gui.LoggieDetailPage, PipelineDetailPanelName)
		}
		return event
	})

	p.gui.AddPanels(gui.YamlDetailPage, text)
	p.gui.SetCurrentPage(gui.YamlDetailPage)
	p.gui.Pages.AddAndSwitchToPage(gui.YamlDetailPage, grid, true)
}

// ---

const (
	filterPending = "pending"
)

type LogStatusPanel struct {
	*tview.TreeView
	gui *gui.Gui

	logConfigName      string
	logConfigNamespace string
	loggieIP           string

	status helper.LogCollectionStatus

	filter      string
	queryStatus string
}

func NewLogStatusPanel(g *gui.Gui, logConfigName string, logConfigNamespace string, podIP string) *LogStatusPanel {
	p := &LogStatusPanel{
		gui:      g,
		TreeView: tview.NewTreeView(),
	}

	p.TreeView.SetBorder(true)
	p.TreeView.SetTitleAlign(tview.AlignCenter)

	p.logConfigName = logConfigName
	p.logConfigNamespace = logConfigNamespace
	p.loggieIP = podIP
	p.SetData()
	p.SetKeybinding(g)
	g.Nav.AddKeyBindingsNavWithKey(LogStatusDetailPanelName, gui.KeyNavFmt("p", "show files collecting"),
		gui.KeyTab, gui.KeyEnter, gui.KeyR, gui.KeyF, gui.KeyQ, gui.KeyJ, gui.KeyK)

	return p
}

func (p *LogStatusPanel) Name() string {
	return LogStatusDetailPanelName
}

func (p *LogStatusPanel) SetData() {
	var pipeline string
	if p.logConfigNamespace == "" {
		pipeline = p.logConfigName
	} else {
		pipeline = fmt.Sprintf("%s/%s", p.logConfigNamespace, p.logConfigName)
	}

	u := url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", p.loggieIP, p.gui.Config.LoggiePort),
		Path:   helper.HandleHelperLogCollection,
	}

	params := url.Values{}
	if pipeline != "" {
		params.Add("pipeline", pipeline)
	}
	if p.queryStatus == filterPending {
		params.Add("status", "pending")
	}
	u.RawQuery = params.Encode()

	cli := http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := cli.Get(u.String())
	if err != nil {
		return
	}
	defer resp.Body.Close()

	out, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	stat := helper.LogCollectionStatus{}
	err = jsoniter.Unmarshal(out, &stat)
	if err != nil {
		return
	}
	p.status = stat

	filesCount := 0
	rootDir := "+-"
	root := tview.NewTreeNode(rootDir).SetColor(tcell.ColorRed)
	for pip, source := range stat.FileStatus.PipelineStat {
		pipeNode := tview.NewTreeNode("Pipeline: " + pip).SetColor(gui.ColorYellow).SetSelectable(true)

		for src, stat := range source.SourceStat {
			srcNode := tview.NewTreeNode("Source: " + src).SetColor(gui.ColorYellow).SetSelectable(true)

			for _, path := range stat.Paths {
				pathNode := tview.NewTreeNode("Path: " + path).SetColor(gui.ColorTeal).SetSelectable(true)
				ignoredNode := tview.NewTreeNode("<ignored>").SetSelectable(true).SetExpanded(false)

				hasIgnored := false
				for _, f := range stat.Detail {
					if !strings.Contains(f.FileName, p.filter) {
						continue
					}

					if ok, _ := util.MatchWithRecursive(path, f.FileName); !ok {
						continue
					}

					base, _ := util.SplitGlobPattern(path)
					filename := strings.TrimPrefix(f.FileName, base)
					filename = strings.TrimPrefix(filename, "/")

					detail := f.FmtDetails()
					node := tview.NewTreeNode(fmt.Sprintf("%s | %s", filename, detail))

					if f.Offset < f.Size {
						node.SetColor(gui.ColorGreen)
					}

					filesCount++
					detailNode := tview.NewTreeNode(f.FileName)
					detailNode.SetSelectable(false)
					node.AddChild(detailNode)
					node.SetExpanded(false)

					if f.Ignored {
						ignoredNode.AddChild(node)
						hasIgnored = true
						continue
					}
					pathNode.AddChild(node)
				}

				if hasIgnored {
					pathNode.AddChild(ignoredNode)
				}
				srcNode.AddChild(pathNode)
			}

			pipeNode.AddChild(srcNode)
		}

		root.AddChild(pipeNode)
	}

	p.TreeView.SetSelectedFunc(func(node *tview.TreeNode) {
		node.SetExpanded(!node.IsExpanded())
	})

	p.TreeView.SetRoot(root).SetCurrentNode(root)

	p.TreeView.SetTitle(fmt.Sprintf(" Log Collection Progress (files: %d, activeFd: %d inactiveFd: %d) ",
		filesCount, p.status.FdStatus.ActiveFdCount, p.status.FdStatus.InActiveFdCount))
}

func (p *LogStatusPanel) UpdateData(g *gui.Gui) {
	go g.App.QueueUpdateDraw(func() {
		p.SetData()
	})
}

func (p *LogStatusPanel) Focus() {
	p.SetBorderColor(gui.ColorYellow)
	p.SetTitleColor(gui.ColorYellow)
	p.gui.App.SetFocus(p.TreeView)
}

func (p *LogStatusPanel) UnFocus() {
	p.SetBorderColor(gui.ColorWhite)
	p.SetTitleColor(gui.ColorWhite)
}

func (p *LogStatusPanel) SetFilterWord(word string) {
	p.filter = word
}

func (p *LogStatusPanel) SetKeybinding(g *gui.Gui) {
	p.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		g.SetGlobalKeybinding(event)

		switch event.Rune() {
		case 'p':
			p.showStatusPending()
		case 'o':
			p.showAll()
		}

		return event
	})
}

func (p *LogStatusPanel) showStatusPending() {
	if p.queryStatus == "" {
		p.queryStatus = filterPending
	} else {
		p.queryStatus = ""
	}
	p.UpdateData(p.gui)
}

func (p *LogStatusPanel) showAll() {
	showAllPanel := NewLogStatusPanel(p.gui, p.logConfigName, p.logConfigNamespace, p.loggieIP)
	showAllPanel.SetBorder(false)

	grid := tview.NewGrid().
		AddItem(showAllPanel.TreeView, 0, 0, 1, 1, 0, 0, true)

	grid.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || event.Rune() == 'q' {
			p.gui.CloseAndSwitchPanel(gui.LoggieProgressPage, gui.LoggieDetailPage, LogStatusDetailPanelName)
		}
		return event
	})

	p.gui.AddPanels(gui.LoggieProgressPage, showAllPanel)
	p.gui.SetCurrentPage(gui.LoggieProgressPage)
	p.gui.Pages.AddAndSwitchToPage(gui.LoggieProgressPage, grid, true)
	p.gui.SwitchPanel(LogStatusDetailPanelName)
}
