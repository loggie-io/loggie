package decompress

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUnZip(t *testing.T) {
	log.InitDefaultLogger()
	ptr := GetOrCreateDecompressorFactory().Get(ZIP)
	zipDecompress := ptr()
	zipDecompressFd, err := zipDecompress.Open("/home/zhanglei/test/test.zip")
	assert.Equal(t, err, nil)
	if err != nil {
		log.Error("%s", err)
		return
	}
	hash, _, err := zipDecompressFd.Hash()
	assert.Equal(t, err, nil)
	if err != nil {
		log.Error("%s", err)
		return
	}
	require.True(t, hash > 0)
	buf := make([]byte, 1024)
	zipDecompressFd.Seek(33 * 1024)
	n, err := zipDecompressFd.Read(buf)
	assert.Equal(t, err, nil)
	require.True(t, n > 0)
	if err != nil {
		log.Error("%s", err)
		return
	}
}

func TestZipReset(t *testing.T) {
	path := "/home/zhanglei/test/test.zip"
	de, err := GetOrCreateDecompressorFactory().MakeDecompressor(path)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, de, nil)
	if de == nil {
		log.Error("err is %s", err)
		return
	}
	if err != nil {
		log.Error("err is %s", err)
		return
	}

	buf := make([]byte, 256)
	n, err := de.Read(buf)
	assert.Equal(t, err, nil)
	require.True(t, n > 0)
	err = de.Reset()
	assert.Equal(t, err, nil)
	if err != nil {
		log.Error("err is %s", err)
	}
	n, err = de.Read(buf)
	assert.Equal(t, err, nil)
	require.True(t, n > 0)
}
