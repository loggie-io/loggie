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

package header

import (
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"strings"
)

type Header struct {
	*tview.TextView
}

var logo = []string{
	`[teal]    __                     _`,
	`[teal]   / /  ____  ____ _____ _(____`,
	"  / /  / [purple]__[teal] \\/ [yellow]__[teal] `/ [lightgreen]__[teal] `/ / _ \\",
	` / /__/ [purple]/_/[teal] / [yellow]/_/[teal] / [lightgreen]/_/[teal] / /  __/`,
	`/_____\____/\__, /\__, /_/\___/`,
	`           /____//____/`,
}

func NewHeader() *Header {
	h := &Header{
		TextView: tview.NewTextView().SetDynamicColors(true),
	}

	h.display()

	return h
}

func (h *Header) display() {
	h.SetTextColor(tcell.ColorSkyblue)

	var out strings.Builder
	for _, l := range logo {
		out.WriteString(l)
		out.WriteString("\n")
	}
	h.SetText(out.String())
}
