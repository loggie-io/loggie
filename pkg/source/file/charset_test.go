package file

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_charset(t *testing.T) {
	gbk := util.AllEncodings["gbk"]
	encode := gbk.NewEncoder()
	decoder := NewCharset("gbk", nil)
	data, err := encode.Bytes([]byte("djwqiodjqiwojdioqwjdioqw"))
	assert.Equal(t, err, nil)
	data, err = decoder.decoder.Bytes(data)
	assert.Equal(t, err, nil)
	fmt.Println(string(data))
}
