package file

import (
	"fmt"
	"github.com/pkg/errors"
)

type Split struct {
	split map[string]string
}

var globalSplit Split

func (split *Split) Init() {
	if split.split == nil {
		split.split = make(map[string]string)
	}
}

func (split *Split) AddSplit(pipelineName string, sourceName string, value string) error {
	if len(value) == 0 {
		return errors.New(fmt.Sprintf("%s:%s split is empty", pipelineName, sourceName))
	}
	key := fmt.Sprintf("%s:%s", pipelineName, sourceName)
	split.split[key] = value
	return nil
}

func (split *Split) GetSplit(pipelineName string, sourceName string) string {
	key := fmt.Sprintf("%s:%s", pipelineName, sourceName)
	value, ok := split.split[key]
	if ok == false {
		return "\n"
	}
	return value
}

func (split *Split) RemoveSplit(pipelineName string, sourceName string) {
	key := fmt.Sprintf("%s:%s", pipelineName, sourceName)
	delete(split.split, key)
}
