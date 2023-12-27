package storage

import (
	"github.com/cockroachdb/pebble"
	"github.com/loggie-io/loggie/pkg/core/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"sync"
	"time"
)

const (
	dbDirStreamPos string = "seiya.streampos.state"
)

var GlobalKeeper *Keeper
var GlobalKeeperMutex sync.RWMutex

// Keeper 保管员用来管理持久化db key的生命周期
type Keeper struct {
	clearInterval time.Duration
	dbStreamIter  *pebble.DB
	storageTable  sync.Map
}

// GetOrCreateKeeper 可能多个cro使用，所以要用单例
func GetOrCreateKeeper() *Keeper {
	if GlobalKeeper != nil {
		return GlobalKeeper
	}
	GlobalKeeperMutex.Lock()
	defer GlobalKeeperMutex.Unlock()
	if GlobalKeeper != nil {
		return GlobalKeeper
	}

	var db *pebble.DB
	var err error
	if db, err = pebble.Open(dbDirStreamPos, nil); err != nil {
		log.Error("pebble.Open error: %s", err)
		return nil
	}

	GlobalKeeper = &Keeper{
		clearInterval: time.Hour * 5,
		dbStreamIter:  db,
	}
	go GlobalKeeper.RunCycleMonitor()
	return GlobalKeeper
}

// RunCycleMonitor 这个函数会定期把所有的key都捞出来,然后回收过期的key
func (k *Keeper) RunCycleMonitor() {
	ticker := time.NewTicker(k.clearInterval)
	saveTicker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			k.recycle()

		case <-saveTicker.C:
			k.storageTable.Range(func(key, value any) bool {
				log.Debug("save %v:%v", key, value)
				k.saveStreamIter(key.(string), value.(*StreamIter))
				k.storageTable.Delete(key)
				return true
			})
		}
	}
}

// GetStreamIter 获取key
func (k *Keeper) GetStreamIter(key string) (iter *StreamIter, err error) {
	var value []byte
	var closer io.Closer
	if value, closer, err = k.dbStreamIter.Get([]byte(key)); err != nil {
		return
	}
	defer closer.Close()
	iter = &StreamIter{}
	if err = proto.Unmarshal(value, iter); err != nil {
		return
	}
	return

}

// SetStreamIter 设置key
func (k *Keeper) SetStreamIter(stream string, iter *StreamIter) (err error) {
	k.storageTable.Store(stream, iter)
	return

}

// SaveStreamIter 迭代器存储落地
func (k *Keeper) saveStreamIter(stream string, iter *StreamIter) (err error) {
	var value []byte
	iter.DataListUptime = timestamppb.New(time.Now())
	if value, err = proto.Marshal(iter); err != nil {
		return
	}
	if err = k.dbStreamIter.Set([]byte(stream), value, nil); err != nil {
		log.Error("k.dbStreamIter.Set error:%s", err)
		return err
	}
	return
}

func (k *Keeper) format(value []byte) (*StreamIter, error) {
	iter := &StreamIter{}
	if err := proto.Unmarshal(value, iter); err != nil {
		return nil, err
	}
	return iter, nil
}

// recycle 从pebble中获取所有key，遍历，清理超时的不活跃的key
func (k *Keeper) recycle() {
	options := &pebble.IterOptions{}
	iter := k.dbStreamIter.NewIter(options)
	for valid := iter.First(); valid; valid = iter.Next() {
		iterData, err := k.format(iter.Value())
		if err != nil {
			log.Error("key(%s) error is : %s", iter.Key(), err)
			continue
		}

		interval := time.Now().Unix() - iterData.DataListUptime.AsTime().Unix()
		if interval < int64(k.clearInterval.Seconds()) {
			continue
		}

		log.Info("delete key:%s", iter.Key())
		writeOptions := &pebble.WriteOptions{}
		k.dbStreamIter.Delete(iter.Key(), writeOptions)
	}
}
