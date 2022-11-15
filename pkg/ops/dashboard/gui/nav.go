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
	"github.com/rivo/tview"
	"strings"
)

const (
	KeyTab    = "tab"
	KeyJ      = "down"
	KeyK      = "up"
	KeyEnter  = "enter"
	KeyQ      = "quit"
	KeyF      = "filter"
	KeyR      = "refresh"
	KeyO      = "show all"
	KeyL      = "logs"
	KeyCtrlF  = "page down"
	KeyCtrlB  = "page up"
	KeyG      = "home"
	KeyShiftG = "end"
)

var defaultKeyBinding = make(map[string]string)

func init() {
	defaultKeyBinding[KeyTab] = KeyNavFmt("tab", "switch between panels")
	defaultKeyBinding[KeyJ] = KeyNavFmt("j", "down")
	defaultKeyBinding[KeyK] = KeyNavFmt("k", "up")
	defaultKeyBinding[KeyEnter] = KeyNavFmt("enter", "show details")
	defaultKeyBinding[KeyQ] = KeyNavFmt("q", "quit")
	defaultKeyBinding[KeyF] = KeyNavFmt("f", "filter")
	defaultKeyBinding[KeyR] = KeyNavFmt("r", "refresh")
	defaultKeyBinding[KeyO] = KeyNavFmt("o", "show all")
	defaultKeyBinding[KeyL] = KeyNavFmt("l", "logs")
	defaultKeyBinding[KeyCtrlF] = KeyNavFmt("ctrl+f", "page down")
	defaultKeyBinding[KeyCtrlB] = KeyNavFmt("ctrl+b", "page up")
	defaultKeyBinding[KeyG] = KeyNavFmt("g", "top")
	defaultKeyBinding[KeyShiftG] = KeyNavFmt("G", "bottom")
}

func KeyNavFmt(key string, info string) string {
	return fmt.Sprintf("[yellow]<%s>:[gray]%s", key, info)
}

type Navigate struct {
	*tview.TextView
	keybindings map[string]string
}

func newNavigate() *Navigate {
	nav := &Navigate{
		TextView:    tview.NewTextView().SetTextColor(tcell.ColorYellow).SetDynamicColors(true),
		keybindings: make(map[string]string),
	}
	return nav
}

func (n *Navigate) AddKeyBindingsNavWithKey(panel string, extra string, keys ...string) {
	var info strings.Builder
	for _, k := range keys {
		val, ok := defaultKeyBinding[k]
		if !ok {
			continue
		}

		info.WriteString(val)
		info.WriteString("  ")
	}

	info.WriteString("\n")
	info.WriteString(extra)
	n.keybindings[panel] = info.String()
}

func (n *Navigate) update(panel string) {
	n.SetText(n.keybindings[panel])
}
