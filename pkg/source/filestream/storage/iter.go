package storage

import (
	"github.com/cockroachdb/pebble"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/pkg/errors"
)

func NewStreamIter(name string) *StreamIter {
	iter, err := GetOrCreateKeeper().GetStreamIter(name)

	if errors.Is(err, pebble.ErrNotFound) {
		return &StreamIter{}
	}

	if err != nil {
		log.Error("name pebble.DB error is %s", err)
		return &StreamIter{}
	}

	return iter
}
