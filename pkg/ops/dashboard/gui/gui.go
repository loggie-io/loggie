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
	"fmt"
	"github.com/gdamore/tcell/v2"
	"github.com/loggie-io/loggie/pkg/core/global"
	"github.com/rivo/tview"
	corev1 "k8s.io/api/core/v1"
)

const (
	TsLayout = "2006-01-02T15:04:05"

	MainPage                   = "main"
	LogConfigDetailPage        = "logConfigDetail"
	LogConfigListPage          = "logConfigList"
	YamlDetailPage             = "logConfigYamlDetail"
	ClusterLogConfigListPage   = "clusterLogConfigList"
	ClusterLogConfigDetailPage = "clusterLogConfigDetail"
	LoggieDetailPage           = "loggieDetail"
	LoggieListPage             = "loggieList"
	LoggieLogsPage             = "loggieLogs"
	LoggieProgressPage         = "loggieProgress"
)

// Considering that different terminals support limited colors, only the most common colors are used here
const (
	ColorSelectedForeground = tcell.ColorBlack

	ColorGreen  = tcell.ColorGreen
	ColorTeal   = tcell.ColorTeal
	ColorWhite  = tcell.ColorWhite
	ColorRed    = tcell.ColorRed
	ColorYellow = tcell.ColorYellow

	ColorTextWhite  = "[white]"
	ColorTextPurple = "[purple]"
	ColorTextTeal   = "[teal]"
)

func NewTableSelectedStyle(color tcell.Color) tcell.Style {
	return tcell.StyleDefault.Background(color).Foreground(ColorSelectedForeground)
}

func TableFocus(table *tview.Table, gui *Gui, color tcell.Color) {
	table.SetSelectable(true, false)
	table.SetBorderColor(color)
	table.SetTitleColor(color)
	gui.App.SetFocus(table)
}

func TableUnFocus(table *tview.Table) {
	table.SetSelectable(false, false)
	table.SetBorderColor(ColorWhite)
	table.SetTitleColor(ColorWhite)
}

func TextViewFocus(t *tview.TextView, gui *Gui, color tcell.Color) {
	t.SetBorderColor(color)
	t.SetTitleColor(color)
	gui.App.SetFocus(t)
}

func TextViewUnFocus(t *tview.TextView) {
	t.SetBorderColor(ColorWhite)
	t.SetTitleColor(ColorWhite)
}

type Gui struct {
	App   *tview.Application
	Pages *tview.Pages

	panels *Panels
	Nav    *Navigate

	K8sClient *K8sClient

	loggieAgentListCache *corev1.PodList
	Config               *Config

	GlobalStat Stat
}

type Config struct {
	LoggiePort int
	KubeConfig string
}

type Stat struct {
	K8sVersion    string
	LoggieVersion string

	LogConfigCount        int
	ClusterLogConfigCount int
	InterceptorCount      int
	SinkCount             int

	LoggiePodCount int
	K8sNodeCount   int
}

func New(config *Config) *Gui {
	return &Gui{
		App:    tview.NewApplication(),
		panels: newPanels(),
		Nav:    newNavigate(),
		Config: config,
	}
}

func (g *Gui) Start() error {
	g.K8sClient = initK8sClient()
	versionInfo, err := g.K8sClient.KubeClient.Discovery().ServerVersion()
	if err != nil {
		return err
	}
	g.GlobalStat.K8sVersion = fmt.Sprintf("%s.%s", versionInfo.Major, versionInfo.Minor)
	g.GlobalStat.LoggieVersion = global.GetVersion()

	return nil
}

func (g *Gui) Stop() {}

func (g *Gui) modal(p tview.Primitive, width, height int) tview.Primitive {
	return tview.NewGrid().
		SetColumns(0, width, 0).
		SetRows(0, height, 0).
		AddItem(p, 1, 1, 1, 1, 0, 0, true)
}

func (g *Gui) SetLoggieAgentList(pod *corev1.PodList) {
	g.loggieAgentListCache = pod
}

func (g *Gui) GetLoggieAgentPod(node string) *corev1.Pod {
	if g.loggieAgentListCache == nil {
		return nil
	}
	for _, p := range g.loggieAgentListCache.Items {
		if p.Spec.NodeName == node {
			return p.DeepCopy()
		}
	}

	return nil
}
