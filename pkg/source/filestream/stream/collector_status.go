package stream

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"sync"
)

type CollectorStatus struct {
	mtx    sync.RWMutex
	enable bool
}

// IsRun 检查是否运行中
func (s *CollectorStatus) IsRun() bool {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if s.enable {
		return true
	}
	return false
}

// Do 执行运行
func (s *CollectorStatus) Do() bool {
	if s.IsRun() {
		return false
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.enable = true
	return true
}

// Finish 完成
func (s *CollectorStatus) Finish() bool {
	if !s.IsRun() {
		log.Info("collector is run")
		return false
	}
	log.Info("collector finish")
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.enable = false
	return true
}
