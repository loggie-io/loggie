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

package dashboard

import (
	"github.com/loggie-io/loggie/pkg/ops/dashboard/clusterlogconfig"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/gui"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/header"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/logconfig"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/loggie"
	"github.com/rivo/tview"
)

type Dashboard struct {
	gui *gui.Gui
}

func New(config *gui.Config) *Dashboard {
	return &Dashboard{
		gui: gui.New(config),
	}
}

func (d *Dashboard) Start() error {
	if err := d.gui.Start(); err != nil {
		return err
	}

	logo := header.NewHeader()
	info := header.NewInfo(d.gui)
	logConfigView := logconfig.NewLogConfigPanel(d.gui)
	clusterLogConfigView := clusterlogconfig.NewClusterLogConfigView(d.gui)
	agentView := loggie.NewAgentView(d.gui)

	d.gui.AddPanels(gui.MainPage, logConfigView, clusterLogConfigView, agentView)

	grid := tview.NewGrid().SetRows(6, 0, 0, 0, 3).
		SetColumns(0, 30).
		AddItem(logo, 0, 0, 1, 1, 0, 0, false).
		AddItem(info, 0, 1, 1, 1, 0, 0, false).
		AddItem(logConfigView.Table, 1, 0, 1, 2, 0, 0, true).
		AddItem(clusterLogConfigView.Table, 2, 0, 1, 2, 0, 0, true).
		AddItem(agentView.Table, 3, 0, 1, 2, 0, 0, true).
		AddItem(d.gui.Nav.TextView, 4, 0, 1, 2, 0, 0, false)

	d.gui.Pages = tview.NewPages().
		AddAndSwitchToPage(gui.MainPage, grid, true)

	d.gui.App.SetRoot(d.gui.Pages, true)
	d.gui.SetCurrentPage(gui.MainPage)
	d.gui.SwitchPanel(logconfig.PanelName)

	if err := d.gui.App.Run(); err != nil {
		d.gui.App.Stop()
		return err
	}

	return nil
}

func (d *Dashboard) Stop() {

}
