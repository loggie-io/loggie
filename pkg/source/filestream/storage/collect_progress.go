package storage

import "time"

// CollectProgress 采集进度
type CollectProgress interface {
	SetName(name string)
	GetName() string
	Reset()
	// GetCollectOffset 获取采集进度
	GetCollectOffset() int64
	// GetCollectHash 获取采集文件hash值
	GetCollectHash() uint64
	// GetCollectMtime 获取采集文件更新时间
	GetCollectMtime() time.Time
	// GetCollectDone 获取采集任务是否完成,只看压缩文件
	GetCollectDone() bool
	// GetCollectSeq 获取采集序列号
	GetCollectSeq() uint64
	GetCollectLine() uint64

	// SetCollectOffset 获取采集进度
	SetCollectOffset(offset int64)
	// AddCollectOffset 添加采集器偏移量
	AddCollectOffset(len int)
	// SetCollectHash 获取采集文件hash值
	SetCollectHash(hash uint64)
	// SetCollectMtime 获取采集文件更新时间
	SetCollectMtime(time.Time)
	// SetCollectDone 获取采集任务是否完成,只看压缩文件
	SetCollectDone(done bool)
	// SetCollectSeq 获取采集序列号
	SetCollectSeq(uint64)
	// SetCollectLine 设置采集进度的行号
	SetCollectLine(uint64)
	// AddCollectLine 递增采集器的行号
	AddCollectLine(uint64)
	// AddCollectSeq 递增采集器的序列号
	AddCollectSeq(uint64)
	// Save 持久化存储进度
	Save() error
	// Debug 调试
	Debug()
	// Copy 复制另一个进度调到这个progress
	Copy(progress CollectProgress)
}
