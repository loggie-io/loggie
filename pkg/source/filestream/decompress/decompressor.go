package decompress

import "path/filepath"

const (
	ZIP = ".zip"
	GZ  = ".gz"
	BZ2 = ".bz2"
	XZ  = ".xz"
)

var (
	COMP_EXTS = []string{GZ, BZ2, XZ, ZIP}
)

type Decompressor interface {
	Open(path string) (Decompressor, error)
	SetHashSize(size int)
	Hash() (uint64, bool, error)
	Size() int64
	Seek(size int64) error
	Read(p []byte) (int, error)
	Close() error
	Reset() error
}

func IsCompressed(fp string) bool {
	ext := filepath.Ext(fp)
	for _, compExt := range COMP_EXTS {
		if ext == compExt {
			return true
		}
	}
	return false
}
