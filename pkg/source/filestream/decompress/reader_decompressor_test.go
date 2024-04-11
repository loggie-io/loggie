package decompress

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func init() {
	log.InitDefaultLogger()
}

func TestUnGZ(t *testing.T) {
	ptr := GetOrCreateDecompressorFactory().Get(GZ)
	zipDecompress := ptr()
	zipDecompressFd, err := zipDecompress.Open("/home/zhanglei/Downloads/genlog/logs/genlog/thread_4/20220721/thread_no_file_name_utf8_1.log.gz")
	assert.Equal(t, err, nil)
	if err != nil {
		log.Error("%s", err)
		return
	}
	zipDecompressFd.SetHashSize(101)
	hash, _, err := zipDecompressFd.Hash()
	assert.Equal(t, err, nil)
	if err != nil {
		log.Error("%s", err)
		return
	}
	fmt.Println(hash)
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

func TestUnBZ2(t *testing.T) {
	log.InitDefaultLogger()
	ptr := GetOrCreateDecompressorFactory().Get(BZ2)
	zipDecompress := ptr()
	zipDecompressFd, err := zipDecompress.Open("/home/zhanglei/Downloads/genlog/logs/genlog/thread_4/20220721/thread_no_file_name_utf8_1.log.bz2")
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
	fmt.Println(hash)
	buf := make([]byte, 1024)
	//zipDecompressFd.Seek(33 * 1024)
	zipDecompressFd.Seek(1000)
	n, err := zipDecompressFd.Read(buf)
	assert.Equal(t, err, nil)
	require.True(t, n > 0)
	if err != nil {
		log.Error("%s", err)
		return
	}
}

func TestUnXZ(t *testing.T) {
	log.InitDefaultLogger()
	ptr := GetOrCreateDecompressorFactory().Get(XZ)
	xzDecompress := ptr()
	xzDecompressFd, err := xzDecompress.Open("/home/zhanglei/Downloads/genlog/logs/genlog/thread_9/20220721/thread_eoi_time_utf8_2022-07-2118-12-46.log.xz")
	assert.Equal(t, err, nil)
	if err != nil {
		log.Error("%s", err)
		return
	}
	xzDecompressFd.SetHashSize(100)
	hash, less, err := xzDecompressFd.Hash()
	assert.Equal(t, less, false)
	require.True(t, hash > 0)
	if err != nil {
		log.Error("%s", err)
		return
	}
	buf := make([]byte, 1024)
	xzDecompressFd.Seek(25)
	n, err := xzDecompressFd.Read(buf)
	assert.Equal(t, err, nil)
	require.True(t, n > 0)
	if err != nil {
		log.Error("%s", err)
		return
	}
}

func TestMakeDecoder(t *testing.T) {
	path := "/home/zhanglei/Downloads/genlog/logs/genlog/thread_9/20220721/thread_eoi_time_utf8_2022-07-2118-12-46.log.xz"
	de, err := GetOrCreateDecompressorFactory().MakeDecompressor(path)
	assert.Equal(t, err, nil)
	if err != nil {
		return
	}
	assert.NotEqual(t, de, nil)
}

func TestReset(t *testing.T) {
	path := "/home/zhanglei/Downloads/genlog/logs/genlog/thread_4/20220721/thread_no_file_name_utf8_1.log.bz2"
	de, err := GetOrCreateDecompressorFactory().MakeDecompressor(path)
	assert.Equal(t, err, nil)
	if de == nil {
		log.Error("err is %s", err)
		return
	}
	if err != nil {
		log.Error("err is %s", err)
		return
	}
	assert.NotEqual(t, de, nil)
	buf := make([]byte, 256)
	n, err := de.Read(buf)
	assert.Equal(t, err, nil)
	err = de.Reset()
	if err != nil {
		log.Error("err is %s", err)
	}
	require.True(t, n > 0)
	n, err = de.Read(buf)
	assert.Equal(t, err, nil)
	require.True(t, n > 0)
}
