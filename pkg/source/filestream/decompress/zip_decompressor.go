package decompress

import (
	"archive/zip"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/pkg/errors"
	"io"
	"io/fs"
)

type ZipDeCompressor struct {
	fd       io.ReadCloser
	zipFd    *zip.ReadCloser
	info     fs.FileInfo
	hashSize int
	offset   uint64
	hash     uint64
	path     string
	zipPath  string
}

func init() {
	GetOrCreateDecompressorFactory().Register(ZIP, makeZipDeCompressor)
}

func makeZipDeCompressor() Decompressor {
	return &ZipDeCompressor{
		hashSize: 256,
	}
}

func (z *ZipDeCompressor) SetHashSize(size int) {
	z.hashSize = size
}

func (z *ZipDeCompressor) Open(path string) (Decompressor, error) {
	var err error
	if z.zipFd, err = zip.OpenReader(path); err != nil {
		return nil, err
	}
	if len(z.zipFd.File) != 1 {
		err = errors.New(fmt.Sprintf("%s contains %d files, however 1 is expected", path, len(z.zipFd.File)))
		z.zipFd.Close()
		return nil, err
	}
	if z.fd, err = z.zipFd.File[0].Open(); err != nil {
		return z, err
	}

	z.info = z.zipFd.File[0].FileInfo()
	z.path = z.zipFd.File[0].Name
	z.zipPath = path
	return z, nil
}

func (z *ZipDeCompressor) Size() int64 {
	return z.info.Size()
}

func (z *ZipDeCompressor) Seek(size int64) error {
	err := z.Reset()

	if err != nil {
		return err
	}

	if size < 0 {
		log.Error("size < 0")
		return errors.New("size < 0")
	}

	if size > z.info.Size() {
		size = z.info.Size()
	}

	if z.offset < uint64(z.hashSize) {
		z.Hash()
	}
	var bufSize int64
	bufSize = 32 * 1024
	for {
		// 剩余等待读取位置
		lastSize := size - int64(z.offset)

		if lastSize < int64(bufSize) {
			bufSize = lastSize
		}

		var buf []byte
		buf = make([]byte, bufSize)

		n, err := z.fd.Read(buf)

		if err != nil {
			z.offset += uint64(n)
			if errors.Is(err, io.EOF) {
				return io.EOF
			} else {
				return err
			}
		}

		z.offset += uint64(n)
		if z.offset >= uint64(size) {
			break
		}
	}
	return nil
}

func (z *ZipDeCompressor) Read(p []byte) (int, error) {
	n, err := z.fd.Read(p)
	z.offset += uint64(n)
	return n, err
}

func (z *ZipDeCompressor) Hash() (uint64, bool, error) {
	if z.hash != 0 {
		return z.hash, false, nil
	}

	if z.info.Size() < int64(z.hashSize) {
		warn := fmt.Sprintf("%s(%d) is less than %d bytes", z.path, z.info.Size(), int64(z.hashSize))
		log.Warn(warn)
		return 0, true, errors.New(warn)
	}

	var buf []byte
	buf = make([]byte, z.hashSize)
	n, err := z.fd.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		log.Error("%s err is %s", z.path, err)
		return 0, false, err
	}

	if n == 0 {
		log.Warn("%s read 0", z.path)
		return 0, true, nil
	}

	if n < z.hashSize {
		return 0, true, nil
	}

	z.hash = xxhash.Sum64(buf)
	z.offset += uint64(z.hashSize)
	return z.hash, false, nil
}

func (z *ZipDeCompressor) Close() error {
	z.offset = 0
	err := z.fd.Close()
	if err != nil {
		log.Error("%s err is %s", z.path, err)
		return err
	}
	err = z.zipFd.Close()
	if err != nil {
		log.Error("%s err is %s", z.path, err)
		return err
	}
	return nil
}

func (z *ZipDeCompressor) Reset() error {
	err := z.Close()
	if err != nil {
		log.Error("%s err is %s", z.path, err)
		return err
	}
	_, err = z.Open(z.zipPath)
	if err != nil {
		log.Error("%s err is %s", z.path, err)
		return err
	}
	return nil
}
