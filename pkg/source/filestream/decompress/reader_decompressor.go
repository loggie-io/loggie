package decompress

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/pkg/errors"
	"github.com/ulikunitz/xz"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

type ReaderDeCompressor struct {
	hashSize int
	offset   uint64
	hash     uint64
	path     string
	info     fs.FileInfo
	reader   io.Reader
	file     *os.File
}

func init() {
	GetOrCreateDecompressorFactory().Register(GZ, makeGZDeCompressor)
	GetOrCreateDecompressorFactory().Register(XZ, makeXZDeCompressor)
	GetOrCreateDecompressorFactory().Register(BZ2, makeBZ2DeCompressor)
}

func makeGZDeCompressor() Decompressor {
	return newReaderDeCompressor()
}

func makeXZDeCompressor() Decompressor {
	return newReaderDeCompressor()
}

func makeBZ2DeCompressor() Decompressor {
	return newReaderDeCompressor()
}

func newReaderDeCompressor() Decompressor {
	return &ReaderDeCompressor{
		hashSize: 256,
	}
}

func (compressor *ReaderDeCompressor) Open(fp string) (Decompressor, error) {
	var err error
	ext := filepath.Ext(fp)
	compressor.file, err = os.Open(fp)
	if err != nil {
		return nil, err
	}
	compressor.info, err = compressor.file.Stat()
	if err != nil {
		return nil, err
	}

	if ext == ".gz" {
		if compressor.reader, err = gzip.NewReader(compressor.file); err != nil {
			return nil, err
		}
	} else if ext == ".bz2" {
		compressor.reader = bzip2.NewReader(compressor.file)
	} else if ext == ".xz" {
		if compressor.reader, err = xz.NewReader(compressor.file); err != nil {
			return nil, err
		}
	} else {
		err = errors.New(fmt.Sprintf("unsupported compress format of %s, supported formats are: .gz, .bz2, .xz, .zip", fp))
		return nil, err
	}
	compressor.path = fp
	return compressor, nil
}

func (compressor *ReaderDeCompressor) Size() int64 {
	return compressor.info.Size()
}

func (compressor *ReaderDeCompressor) SetHashSize(size int) {
	compressor.hashSize = size
}

func (compressor *ReaderDeCompressor) Hash() (uint64, bool, error) {
	if compressor.hash != 0 {
		return compressor.hash, false, nil
	}

	var buf []byte
	buf = make([]byte, compressor.hashSize)
	n, err := compressor.reader.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		log.Error("%s err is %s", compressor.path, err)
		return 0, false, err
	}

	if n < compressor.hashSize {
		warn := fmt.Sprintf("%s(%d) is less than %d bytes", compressor.path, compressor.info.Size(), int64(compressor.hashSize))
		log.Warn(warn)
		return 0, true, errors.New(warn)
	}

	if n == 0 {
		log.Warn("%s read 0", compressor.path)
		return 0, true, err
	}

	compressor.hash = xxhash.Sum64(buf)
	compressor.offset += uint64(compressor.hashSize)
	return compressor.hash, false, nil
}

func (compressor *ReaderDeCompressor) Seek(size int64) error {
	err := compressor.Reset()

	if size == 0 {
		return nil
	}

	if size < 0 {
		log.Error("size < 0")
		return errors.New("size < 0")
	}

	if err != nil {
		return err
	}

	var bufSize int64
	bufSize = 32 * 1024
	for {
		// 剩余等待读取位置
		lastSize := size - int64(compressor.offset)

		var readSize int64
		if lastSize < int64(bufSize) {
			readSize = lastSize
		} else {
			readSize = bufSize
		}

		var buf []byte
		buf = make([]byte, readSize)

		n, err := compressor.reader.Read(buf)

		if err != nil {
			compressor.offset += uint64(n)
			if errors.Is(err, io.EOF) {
				return io.EOF
			} else {
				return err
			}
		}

		compressor.offset += uint64(n)

		if compressor.offset >= uint64(size) {
			break
		}
	}
	return nil
}

func (compressor *ReaderDeCompressor) Read(p []byte) (int, error) {
	n, err := compressor.reader.Read(p)
	compressor.offset += uint64(n)
	return n, err
}

func (compressor *ReaderDeCompressor) Close() error {
	compressor.offset = 0
	err := compressor.file.Close()
	return err
}

func (compressor *ReaderDeCompressor) Reset() error {
	err := compressor.Close()
	if err != nil {
		return err
	}
	_, err = compressor.Open(compressor.path)
	if err != nil {
		return err
	}
	return nil
}
