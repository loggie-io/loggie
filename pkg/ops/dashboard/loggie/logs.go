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
	"bufio"
	"context"
	"github.com/gdamore/tcell/v2"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/gui"
	"github.com/rivo/tview"
	"io"
	corev1 "k8s.io/api/core/v1"
	"time"
)

const (
	LogPanelName = "ShowLogs"

	tailLines = 500
)

type LogPanel struct {
	*tview.TextView
	gui *gui.Gui

	print io.Writer

	name      string
	namespace string

	dataChan chan []byte
	ctx      context.Context
}

func NewLoggieLogPanel(g *gui.Gui, namespace string, name string) *LogPanel {
	p := &LogPanel{
		gui:      g,
		TextView: tview.NewTextView(),
		dataChan: make(chan []byte, 100),
		ctx:      context.Background(),
	}

	p.TextView.SetRegions(true).SetScrollable(true).SetDynamicColors(true)
	p.TextView.SetChangedFunc(func() {
		g.App.Draw()
	})

	p.print = tview.ANSIWriter(p.TextView)

	p.name = name
	p.namespace = namespace

	go p.fetchLogs(p.ctx, p.namespace, p.name)
	p.SetData()
	p.SetKeybinding(g)

	return p
}

func (p *LogPanel) Name() string {
	return LogPanelName
}

func (p *LogPanel) fetchLogs(ctx context.Context, namespace string, name string) {
	tail := int64(tailLines)
	req := p.gui.K8sClient.KubeClient.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{
		Follow:    true,
		TailLines: &tail,
	}).Timeout(5 * time.Second)

	stream, err := req.Stream(ctx)
	if err != nil {
		p.dataChan <- []byte(err.Error())
		return
	}
	defer stream.Close()

	r := bufio.NewReader(stream)
	for {
		select {
		case <-ctx.Done():
			break

		default:
		}

		content, err := r.ReadBytes('\n')
		if err != nil {
			p.dataChan <- []byte(err.Error())
		}

		p.dataChan <- content
	}
}

func (p *LogPanel) SetData() {
	go func() {
		p.TextView.ScrollToEnd()
		for l := range p.dataChan {
			p.print.Write(l)
		}
	}()
}

func (p *LogPanel) UpdateData(g *gui.Gui) {
	go g.App.QueueUpdateDraw(func() {
		p.SetData()
	})
}

func (p *LogPanel) Focus() {
	p.gui.App.SetFocus(p.TextView)
}

func (p *LogPanel) UnFocus() {
}

func (p *LogPanel) SetFilterWord(word string) {
}

func (p *LogPanel) SetKeybinding(g *gui.Gui) {
	p.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		g.SetGlobalKeybinding(event)
		return event
	})
}
