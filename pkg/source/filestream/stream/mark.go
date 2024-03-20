package stream

import (
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/source/filestream/ack"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
	"strconv"
	"strings"
)

type Mark struct {
	progress storage.CollectProgress
	epoch    *pipeline.Epoch
	uuid     string
	name     string
	source   string
}

// NewMark 初始化流ack标记
// progress 用来生成ackid
// mark 用来判断是否标记进度
func NewMark(progress storage.CollectProgress, source string, epoch *pipeline.Epoch, mark bool) ack.Mark {
	block := &Mark{
		epoch: epoch,
		name:  progress.GetName(),
	}

	if mark {
		// 拷贝进度到ack块
		ackProgress := NewStreamProgress(progress.GetName())
		ackProgress.Copy(progress)
		block.progress = ackProgress
	}

	//生成标识号码
	var uuid strings.Builder
	uuid.WriteString(progress.GetName())
	uuid.WriteString("-")
	uuid.WriteString(strconv.FormatUint(progress.GetCollectHash(), 10))
	uuid.WriteString("-")
	uuid.WriteString(strconv.FormatInt(progress.GetCollectOffset(), 10))
	block.uuid = uuid.String()
	block.source = source
	return block
}

// GetAckId 获取Ack模块标识
func (m *Mark) GetAckId() string {
	return m.uuid
}

// GetProgress 获取Ack进度
func (m *Mark) GetProgress() storage.CollectProgress {
	return m.progress
}

// GetName 流的名字
func (m *Mark) GetName() string {
	return m.name
}

// GetSourceName 流的名字
func (m *Mark) GetSourceName() string {
	return m.source
}

// GetName 流的名字
func (m *Mark) GetEpoch() *pipeline.Epoch {
	return m.epoch
}
