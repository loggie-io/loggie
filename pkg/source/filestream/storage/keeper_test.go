package storage

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/loggie-io/loggie/pkg/core/log"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

func init() {
	log.InitDefaultLogger()
}

func TestKeeper(t *testing.T) {
	Keeper := GetOrCreateKeeper()

	iter := &StreamIter{
		Offset: 1,
	}
	Keeper.SetStreamIter("keep1", iter)

	iter = &StreamIter{
		Offset: 2,
	}
	Keeper.SetStreamIter("keep2", iter)

	iter = &StreamIter{
		Offset: 3,
	}
	Keeper.SetStreamIter("keep3", iter)

	options := &pebble.IterOptions{}
	keeperIter := Keeper.dbStreamIter.NewIter(options)
	for valid := keeperIter.First(); valid; valid = keeperIter.Next() {
		value := keeperIter.Value()
		iter = &StreamIter{}
		if err := proto.Unmarshal(value, iter); err != nil {
			return
		}
		fmt.Println(iter)
		fmt.Println(string(keeperIter.Key()))
	}

	Keeper.clearInterval = 1 * time.Second
	time.Sleep(1000)
}
