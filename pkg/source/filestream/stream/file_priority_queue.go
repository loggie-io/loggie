package stream

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/filestream/config"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
	"github.com/loggie-io/loggie/pkg/source/filestream/util"
	"github.com/pkg/errors"
	"sync/atomic"
)

type FilesPriorityQueue struct {
	pattern    string
	progress   storage.CollectProgress
	running    uint32
	fileConfig *config.FileConfig
	queue      *PriorityQueue
}

// NewFilesPriorityQueue 初始化文件列表
func NewFilesPriorityQueue(pattern string, maxSize int, config *config.FileConfig, progress storage.CollectProgress) FilestreamList {
	return &FilesPriorityQueue{
		pattern:    pattern,
		progress:   progress,
		fileConfig: config,
		running:    1,
		queue:      NewStreamPriorityQueue(maxSize),
	}
}

// Init 生成流列表
func (s *FilesPriorityQueue) Init() error {
	s.queue.Reset()
	_, err := util.Glob(s.pattern, true, nil, func(path string) error {

		if atomic.LoadUint32(&s.running) == 0 {
			return HasStop
		}

		// 流列表出现变化, 生成采集文件信息的时候文件被删了可能
		file, err := NewStreamFile(path, s.fileConfig)
		if err != nil {
			log.Error("NewStreamFile(%s) error:%s", path, err)
			return nil
		}

		iterMtime := s.progress.GetCollectMtime().Unix()

		if iterMtime > 0 && iterMtime > file.GetMTimeUnix() {
			return nil
		}

		// 生成Hash，如果文件内容小于指定hash尺寸的不采集
		_, err = file.GetHash()
		if err != nil {
			log.Error("GetHash(%s) error:%s", path, err)
			return nil
		}

		//s.list = append(s.list, file)
		s.queue.SureEnQueue(&PriorityQueueData{
			key:   file.GetMTimeUnix(),
			value: file,
		})

		return nil
	})

	if err != nil {
		log.Error("s.HasScroll error:%s", err)
		return err
	}

	if (s.queue.Size()) == 0 {
		return IsEmpty
	}

	return nil
}

// HasScroll 判断是否发生了滚动
// 判断规则是流列表的文件中的最早的文件
// 1、不存在了，说明流滚动了
// 2、Hash 变掉了，说明采集的文件内容发生变化
// 以上情况都认为流发生了改变，需要重新生成采集列表
func (s *FilesPriorityQueue) HasScroll() error {
	if s.queue.Size() == 0 {
		return IsEmpty
	}

	front := s.queue.GetQueueTop()
	file := front.value.(*File)
	if file == nil {
		return errors.New("front value si nil")
	}

	err := file.HasChange()

	// 文件发生变动
	if errors.Is(err, FileChangeError) {
		return HasChange
	}

	if err != nil {
		log.Error("s.HasScroll error:%s", err)
		return err
	}

	return nil
}

// Select 从流了表中选择一个需要采集的流文件
// 并且选择后进行inode 和 dev进行检查
// 如果发生变化，代表要采集的目标文件发生变化，要重新采集
func (s *FilesPriorityQueue) Select() (*File, error) {
	file, err := s.selectFile()

	if err != nil {
		return file, err
	}

	// 在open之前，检查inode是否发生了变化
	// 如果发生了mv一类的变化要重新生成文件列表
	err = file.FileInfoHasChange()
	if err != nil {
		return file, err
	}

	return file, nil
}

// selectFile 从流了表中选择一个需要采集的流文件
func (s *FilesPriorityQueue) selectFile() (*File, error) {
	if s.queue.Size() == 0 {
		return nil, IsEmpty
	}

	if atomic.LoadUint32(&s.running) == 0 {
		return nil, HasStop
	}

	// ZL:流列表不稳定，发生了变化不能继续采集
	if err := s.HasScroll(); err != nil {
		return nil, err
	}

	//没有采集过直接找第一个文件
	front := s.queue.GetQueueTop()
	if front == nil {
		return nil, errors.New("queue front is nil")
	}
	file := front.value.(*File)
	if s.progress.GetCollectHash() == 0 {
		hash, err := file.GetHash()
		if err != nil {
			log.Error("Get Hash error:", err)
			return nil, err
		}
		s.progress.Reset()
		s.progress.SetCollectHash(hash)
		return file, nil
	}

	// ZL:从流上找到对应的文件位置
	var findHash bool = false
	for {

		if atomic.LoadUint32(&s.running) == 0 {
			return nil, HasStop
		}

		data := s.queue.DeQueue()
		// 找到了流文件hash，但是文件列表已经走到末尾了
		// 这种情况可能是列表里最后一个文件并且被处理完了
		if data == nil && findHash {
			return nil, nil
		}

		// 没找到hash,列表已经搜索完了
		if data == nil {
			break
		}

		queueFile := data.value.(*File)
		// 所有文件都采集完了
		if queueFile == nil {
			return nil, nil
		}
		hash, err := queueFile.GetHash()
		// ZL: 如果流文件比hashSize 小，我们跳过当前的文件，继续采集，避免因为一个小文件，阻塞流列表的采集
		if errors.Is(err, HashSizeSmallError) {
			continue
		}

		// ZL: 从文件列表中找寻迭代器的hash
		if s.progress.GetCollectHash() == hash {
			findHash = true
			//对比要采集的文件是否被采集完成
			if s.progress.GetCollectOffset() < queueFile.GetSize() {
				return queueFile, nil
			}

			// ZL: 如果采集目标文件比偏移量小，那么调整迭代器为文件大小
			if s.progress.GetCollectOffset() > queueFile.GetSize() {
				log.Warn("%s s.progress.GetCollectOffset(%d) > file.GetSize(%d)", queueFile.path,
					s.progress.GetCollectOffset(), queueFile.GetSize())
				s.progress.SetCollectOffset(queueFile.GetSize())
				return file, nil
			}

			// 采集完了，直接返回下一个节点进行采集
			data = s.queue.DeQueue()
			// 已经是最后一个文件了
			if data == nil {
				return nil, nil
			}

			queueFile = data.value.(*File)
			hash, _ = queueFile.GetHash()
			s.progress.Reset()
			s.progress.SetCollectHash(hash)
			return queueFile, nil
		}

	}

	// 到达这里的可能性是没找到hash,那么之前的迭代器必须要失效，使用新的迭代器
	hash, _ := file.GetHash()

	s.progress.Reset()
	s.progress.SetCollectHash(hash)
	s.progress.SetCollectMtime(file.mtime)
	return file, nil
}

// GetListLen 获取文件流列表长度
func (s *FilesPriorityQueue) GetListLen() int {
	return s.queue.Size()
}

// IsFinish 检查流列表是否采集完成
// 判断标准是尾部的文件是否是迭代器的Hash,以及对应的Hash文件采集进度是否处于尾部
func (s *FilesPriorityQueue) IsFinish() (bool, error) {
	if s.queue.Size() == 0 {
		return true, nil
	}

	file := s.queue.Front().value.(*File)

	hash, err := file.GetHash()
	if err != nil {
		log.Error("%v hash error:%s", file, err)
		return false, err
	}

	if s.progress.GetCollectHash() != hash {
		return false, err
	}

	if s.progress.GetCollectOffset() > file.GetSize() {
		s.progress.SetCollectOffset(file.GetSize())
		return true, nil
	}

	if s.progress.GetCollectOffset() == file.GetSize() {
		return true, nil
	}

	return false, nil
}

func (s *FilesPriorityQueue) Stop() {
	atomic.StoreUint32(&s.running, 0)
}
