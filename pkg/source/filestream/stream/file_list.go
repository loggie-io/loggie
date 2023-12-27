package stream

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/filestream/config"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
	"github.com/loggie-io/loggie/pkg/source/filestream/util"
	"github.com/pkg/errors"
	"sort"
	"sync/atomic"
)

var HasChange = errors.Errorf("flow file has changed")
var IsEmpty = errors.Errorf("file stream is empty")
var HasStop = errors.Errorf("file stream has stop")

type FilesList struct {
	pattern    string
	progress   storage.CollectProgress
	list       []*File
	running    uint32
	fileConfig *config.FileConfig
}

// NewStreamFileList 初始化文件列表
func NewStreamFileList(pattern string, config *config.FileConfig, progress storage.CollectProgress) FilestreamList {
	return &FilesList{
		pattern:    pattern,
		progress:   progress,
		fileConfig: config,
		running:    1,
	}
}

// Init 生成流列表
func (s *FilesList) Init() error {
	s.list = []*File{}

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

		s.list = append(s.list, file)

		return nil
	})

	if err != nil {
		log.Error("s.HasScroll error:%s", err)
		return err
	}

	if len(s.list) == 0 {
		return IsEmpty
	}

	// 按时间从小到达排序整个列表
	sort.Slice(s.list, func(i, j int) bool {
		return s.list[i].mtime.Unix() < s.list[j].mtime.Unix()
	})

	return nil
}

// HasScroll 判断是否发生了滚动
// 判断规则是流列表的文件中的最早的文件
// 1、不存在了，说明流滚动了
// 2、Hash 变掉了，说明采集的文件内容发生变化
// 以上情况都认为流发生了改变，需要重新生成采集列表
func (s *FilesList) HasScroll() error {
	if len(s.list) == 0 {
		return IsEmpty
	}

	file := s.list[0]

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
func (s *FilesList) Select() (*File, error) {
	if len(s.list) == 0 {
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
	if s.progress.GetCollectHash() == 0 {
		hash, err := s.list[0].GetHash()
		if err != nil {
			log.Error("Get Hash error:", err)
			return nil, err
		}
		s.progress.Reset()
		s.progress.SetCollectHash(hash)
		return s.list[0], nil
	}

	listLen := len(s.list)
	// ZL:从流上找到对应的文件位置
	for i := 0; i < listLen; i++ {

		if atomic.LoadUint32(&s.running) == 0 {
			return nil, HasStop
		}

		file := s.list[i]
		hash, err := file.GetHash()
		// ZL: 如果流文件比hashSize 小，我们跳过当前的文件，继续采集，避免因为一个小文件，阻塞流列表的采集
		if errors.Is(err, HashSizeSmallError) {
			continue
		}

		// ZL: 从文件列表中找寻迭代器的hash
		if s.progress.GetCollectHash() == hash {
			//对比要采集的文件是否被采集完成
			if s.progress.GetCollectOffset() < file.GetSize() {
				return file, nil
			}

			// ZL: 如果采集目标文件比偏移量小，那么调整迭代器为文件大小
			if s.progress.GetCollectOffset() > file.GetSize() {
				log.Warn("%s s.progress.GetCollectOffset(%d) > file.GetSize(%d)", file.path,
					s.progress.GetCollectOffset(), file.GetSize())
				s.progress.SetCollectOffset(file.GetSize())
				return file, nil
			}

			// ZL: 如果采集的目标被采集完成，那么判断他是不是处于列表的尾部,如果处于列表的尾部，不返回任何文件，就要结束这次采集任务了
			// 如果采集的目标文件不处于流列表尾部,那么返回对应的file
			if i == (listLen - 1) {
				return nil, nil
			}

			i++
			s.progress.Reset()
			s.progress.SetCollectHash(hash)
			return s.list[i], nil
		}

	}

	// 到达这里的可能性是没找到hash,那么之前的迭代器必须要失效，使用新的迭代器
	file := s.list[0]
	hash, _ := file.GetHash()

	s.progress.Reset()
	s.progress.SetCollectHash(hash)
	s.progress.SetCollectMtime(file.mtime)
	log.Info("hash(%v) is not found", s.progress.GetCollectHash())
	return file, nil
}

// GetListLen 获取文件流列表长度
func (s *FilesList) GetListLen() int {
	return len(s.list)
}

// IsFinish 检查流列表是否采集完成
// 判断标准是尾部的文件是否是迭代器的Hash,以及对应的Hash文件采集进度是否处于尾部
func (s *FilesList) IsFinish() (bool, error) {
	if len(s.list) == 0 {
		return true, nil
	}

	file := s.list[len(s.list)-1]

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

func (s *FilesList) Stop() {
	atomic.StoreUint32(&s.running, 0)
}
