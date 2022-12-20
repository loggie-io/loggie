package journal

import "github.com/loggie-io/loggie/pkg/core/api"

type sortableEvents []api.Event

func (list sortableEvents) Len() int {
	return len(list)
}

func (list sortableEvents) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

func (list sortableEvents) Less(i, j int) bool {
	return getState(list[i]).Offset > getState(list[j]).Offset
}
