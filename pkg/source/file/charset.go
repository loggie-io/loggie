package file

import (
	"errors"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util"
	"golang.org/x/text/encoding"
)

type CharsetDecoder struct {
	decoder     *encoding.Decoder
	charset     string
	productFunc api.ProductFunc
}

func NewCharset(charset string, productFunc api.ProductFunc) *CharsetDecoder {
	var decoder *encoding.Decoder
	codec, ok := util.AllEncodings[charset]
	if !ok {
		decoder = util.AllEncodings["utf-8"].NewDecoder()
		log.Warn("The encoding(%s) does not exist, it has been converted to utf-8 by default", charset)
	}
	decoder = codec.NewDecoder()
	return &CharsetDecoder{
		decoder:     decoder,
		charset:     charset,
		productFunc: productFunc,
	}
}

func (i *CharsetDecoder) process(e api.Event) error {
	if i.charset == "utf-8" {
		return nil
	}

	bytes, err := i.decoder.Bytes(e.Body())

	if err != nil {
		log.Error("failed to iconv  into %v", i.charset)
		return errors.New(fmt.Sprintf("failed to encode  into %v", i.charset))
	}

	e.Fill(e.Meta(), e.Header(), bytes)
	return nil
}

func (i *CharsetDecoder) Hook(event api.Event) api.Result {
	err := i.process(event)
	if err != nil {
		log.Error("i.process error:%s", err)
	}
	return i.productFunc(event)
}
