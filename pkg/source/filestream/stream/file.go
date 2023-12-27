package stream

import (
	"github.com/cespare/xxhash/v2"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/filestream/config"
	"github.com/loggie-io/loggie/pkg/source/filestream/decompress"
	"github.com/loggie-io/loggie/pkg/source/filestream/process/define"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"time"
)

var FileChangeError = errors.Errorf("FIle has change")
var HashSizeSmallError = errors.Errorf("FIle hash size small")

type File struct {
	path         string
	mtime        time.Time
	hash         uint64
	hashSize     int64
	size         int64
	maxReadSize  int64
	decompressor decompress.Decompressor
	inode        uint64
	dev          uint64
}

// NewStreamFile 生成刘文件对象
func NewStreamFile(path string, config *config.FileConfig) (*File, error) {

	var fi os.FileInfo

	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	stat := fi.Sys().(*syscall.Stat_t)

	file := &File{
		path:        path,
		mtime:       fi.ModTime(),
		hashSize:    config.HashSize,
		size:        fi.Size(),
		maxReadSize: config.MaxReadSize,
		inode:       stat.Ino,
		dev:         stat.Dev,
	}

	// 判断是否是压缩文件,如果是压缩文件需要初始化压缩文件解析器
	if decompress.IsCompressed(path) {
		file.decompressor, err = decompress.GetOrCreateDecompressorFactory().MakeDecompressor(path)
		if err != nil {
			return nil, err
		}

		file.decompressor.SetHashSize(int(config.HashSize))
	}

	return file, nil
}

func (f *File) GetMTimeUnix() int64 {
	return f.mtime.Unix()
}

func (f *File) GetSize() int64 {
	return f.size
}

// GetSysStatInfo 从内核中获取文件信息
func (f *File) GetSysStatInfo() (os.FileInfo, *syscall.Stat_t, error) {
	var fi os.FileInfo

	fi, err := os.Stat(f.path)
	if err != nil {
		return nil, nil, err
	}

	stat := fi.Sys().(*syscall.Stat_t)
	return fi, stat, nil
}

// HasChange 判断文件是否发生了变动
// 文件是否被删除
// Mtime 是否发生了变动
func (f *File) HasChange() error {
	fi, sys, err := f.GetSysStatInfo()

	// 文件被删掉了
	if errors.Is(err, unix.ENOENT) {
		return FileChangeError
	}

	if err != nil {
		log.Error("Path(%s) Has Change err:%s", f.path, err)
		return err
	}

	// 文件变动了
	if (fi.ModTime().Unix()) != f.mtime.Unix() {
		return FileChangeError
	}

	// 检查文件是否是inode是的话就跳过校验
	if sys.Ino != f.inode {
		return FileChangeError
	}

	return nil
}

// FileInfoHasChange 判断文件系统信息是否发生了变动，主要为了应对mv
// 忽略mtime的检查
// 文件是否被删除
// Dev_t是否变动
// Inode是否变动
func (f *File) FileInfoHasChange() error {
	_, sys, err := f.GetSysStatInfo()

	// 文件被删掉了
	if errors.Is(err, unix.ENOENT) {
		return FileChangeError
	}

	if err != nil {
		log.Error("Path(%s) Has Change err:%s", f.path, err)
		return err
	}

	// 检查文件inode是否变化
	if sys.Ino != f.inode {
		return FileChangeError
	}

	// 检查文件dev是否变化
	if sys.Dev != f.dev {
		return FileChangeError
	}

	return nil
}

// GetHash 获取文件hash值
// 如果说文件有hash值那么用之前的,没有的话则读取现在的
func (f *File) GetHash() (uint64, error) {
	var err error
	if f.hash != 0 {
		return f.hash, nil
	}

	if f.size < f.hashSize {
		log.Error("path(%s) read not enough bytes:%v < %v", f.path, f.size, f.hashSize)
		return 0, HashSizeSmallError
	}

	if f.decompressor == nil {
		var handler *os.File
		handler, err := os.Open(f.path)
		if err != nil {
			return 0, err
		}
		defer handler.Close()
		buf := make([]byte, f.hashSize)
		var n int
		n, err = handler.Read(buf)
		if err != nil {
			log.Error("path(%s) read error:%s", f.path, err)
			return 0, err
		}

		// 读取字节不足，无法进行hash
		if n < int(f.hashSize) {
			log.Error("path(%s) read not enough bytes:%v < %v", f.path, n, f.hashSize)
			return 0, HashSizeSmallError
		}
		f.hash = xxhash.Sum64(buf)
		return f.hash, nil
	}

	var less bool
	f.hash, less, err = f.decompressor.Hash()
	if err != nil {
		log.Error("path(%s) read error:%s", f.path, err)
		return 0, err
	}

	if less == true {
		log.Error("path(%s) read not enough bytes:%v < %v", f.path, f.size, f.hashSize)
		return 0, HashSizeSmallError
	}

	return f.hash, err
}

// compressRead 压缩文件采集
func (f *File) compressRead(processChain define.ChainProcess, collector *Collector) (err error) {
	ctx := NewStreamContext()
	ctx.SetHash(f.hash)
	ctx.SetPath(f.path)
	if processChain.GetProgress().GetCollectDone() == true {
		processChain.OnComplete(ctx)
		return
	}
	buf := make([]byte, f.maxReadSize)
	err = f.decompressor.Seek(processChain.GetProgress().GetCollectOffset())
	if err != nil {
		processChain.OnError(err)
		return err
	}

	for {
		if atomic.LoadUint32(&collector.running) == 0 {
			return HasStop
		}
		n, err := f.decompressor.Read(buf)
		buf = buf[:n]

		if n > 0 {
			ctx.SetBuf(buf)
			processChain.OnProcess(ctx)
			continue
		}

		if errors.Is(err, io.EOF) {
			ctx.SetBuf([]byte{})
			processChain.GetProgress().SetCollectDone(true)
			processChain.OnComplete(ctx)
			return err
		}

		if err != nil {
			processChain.OnError(err)
			return err
		}
	}
}

// normalRead 非压缩文件读取
func (f *File) normalRead(processChain define.ChainProcess, collector *Collector) (err error) {
	stat, _, err := f.GetSysStatInfo()
	if err != nil {
		log.Error("f.path GetSysStatInfo err:%s", err)
		processChain.OnError(err)
		return
	}

	offset := processChain.GetProgress().GetCollectOffset()
	ctx := NewStreamContext()
	ctx.SetPath(f.path)
	ctx.SetHash(f.hash)
	// 已经读完了
	if stat.Size() == offset {
		ctx.SetBuf([]byte{})
		processChain.OnComplete(ctx)
		return
	}

	// 少于偏移量
	if stat.Size() < offset {
		ctx.SetBuf([]byte{})
		processChain.OnComplete(ctx)
		return
	}

	// 读取偏移量代码
	var fd *os.File
	fd, err = os.Open(f.path)
	if err != nil {
		return
	}

	defer fd.Close()

	var n int
	buf := make([]byte, f.maxReadSize) //1MB

	for {
		if atomic.LoadUint32(&collector.running) == 0 {
			return HasStop
		}
		ctx.ResetMarkLastLine()
		n, err = fd.ReadAt(buf, offset)
		offset += int64(n)
		buf = buf[:n]

		if n > 0 {
			ctx.SetBuf(buf)
			processChain.OnProcess(ctx)
			continue
		}

		if errors.Is(err, io.EOF) {
			ctx.SetBuf([]byte{})
			processChain.OnComplete(ctx)
			return
		}

		if err != nil {
			processChain.OnError(err)
			return
		}
	}
}

// Collect 读取会自动识别压缩文件和非压缩文件
func (f *File) Collect(processChain define.ChainProcess, collector *Collector) (err error) {
	if f.decompressor == nil {
		return f.normalRead(processChain, collector)
	}

	return f.compressRead(processChain, collector)
}
