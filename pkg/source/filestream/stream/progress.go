package stream

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sync"
	"time"
)

// StreamProgress 流处理进度
type StreamProgress struct {
	name  string
	data  *storage.StreamIter
	mutex sync.RWMutex
	db    *storage.Keeper
}

func NewStreamProgress(name string) storage.CollectProgress {
	return &StreamProgress{
		name: name,
		data: storage.NewStreamIter(name),
		db:   storage.GetOrCreateKeeper(),
	}
}

// Reset 重置采集进度
func (s *StreamProgress) Reset() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data.Reset()
}

func (s *StreamProgress) SetName(name string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.name = name
}

func (s *StreamProgress) GetName() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.name
}

// GetCollectOffset 获取采集进度
func (s *StreamProgress) GetCollectOffset() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.data.Offset
}

// GetCollectHash 获取采集文件hash值
func (s *StreamProgress) GetCollectHash() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.data.Hash
}

// GetCollectMtime 获取采集文件更新时间
func (s *StreamProgress) GetCollectMtime() time.Time {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.data.Mtime.AsTime()
}

// GetCollectDone 获取采集任务是否完成,只看压缩文件
func (s *StreamProgress) GetCollectDone() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.data.Done
}

// GetCollectSeq 获取采集序列号
func (s *StreamProgress) GetCollectSeq() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.data.Seq
}
func (s *StreamProgress) GetCollectLine() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.data.Line
}

// SetCollectOffset 获取采集进度
func (s *StreamProgress) SetCollectOffset(offset int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data.Offset = offset
}

// SetCollectHash 获取采集文件hash值
func (s *StreamProgress) SetCollectHash(hash uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data.Hash = hash
}

// SetCollectMtime 获取采集文件更新时间
func (s *StreamProgress) SetCollectMtime(mtime time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data.Mtime = timestamppb.New(mtime)
}

// SetCollectDone 获取采集任务是否完成,只看压缩文件
func (s *StreamProgress) SetCollectDone(done bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data.Done = done
}

// SetCollectSeq 获取采集序列号
func (s *StreamProgress) SetCollectSeq(seq uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data.Seq = seq
}

// SetCollectLine 设置采集进度的行号
func (s *StreamProgress) SetCollectLine(line uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data.Line = line
}

// AddCollectLine 递增采集器的行号
func (s *StreamProgress) AddCollectLine(line uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data.Line += line
}

// AddCollectSeq 递增采集器的序列号
func (s *StreamProgress) AddCollectSeq(seq uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data.Seq += seq
}

func (s *StreamProgress) AddCollectOffset(size int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data.Offset += int64(size)
}

// Debug 递增采集器的序列号
func (s *StreamProgress) Debug() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	fmt.Println(s.data)
}

// Save 持久化存储进度
func (s *StreamProgress) Save() error {
	return s.db.SetStreamIter(s.name, s.data)
}

// Copy 持久化存储进度
func (s *StreamProgress) Copy(progress storage.CollectProgress) {
	s.SetCollectHash(progress.GetCollectHash())
	s.SetCollectMtime(progress.GetCollectMtime())
	s.SetCollectOffset(progress.GetCollectOffset())
	s.SetCollectDone(progress.GetCollectDone())
	s.SetCollectLine(progress.GetCollectLine())
	s.SetCollectSeq(progress.GetCollectSeq())
	return
}
