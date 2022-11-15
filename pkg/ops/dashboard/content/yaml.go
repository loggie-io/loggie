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
	"bufio"
	"fmt"
	"github.com/loggie-io/loggie/pkg/ops/dashboard/gui"
	"strings"
)

func YamlHighlight(str string, filter string) string {
	var highlighted strings.Builder
	r := bufio.NewReader(strings.NewReader(str))
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}

		if filter != "" && strings.Contains(line, filter) {
			line = strings.ReplaceAll(line, filter, fmt.Sprintf("[red]%s[white]", filter))
		}

		splits := strings.SplitN(line, ":", 2)
		if len(splits) < 2 {
			highlighted.WriteString(line)
			continue
		}

		colorWords := gui.ColorTextTeal + splits[0] + gui.ColorTextWhite
		highlighted.WriteString(colorWords)
		highlighted.WriteString(":")
		for i := 1; i < len(splits); i++ {
			highlighted.WriteString(splits[i])
		}
	}
	return highlighted.String()
}
