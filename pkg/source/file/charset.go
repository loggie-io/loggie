/*
Copyright 2022 Loggie Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	} else {
		decoder = codec.NewDecoder()
	}
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
		log.Error("encoding conversion failed %s", i.charset)
		return errors.New(fmt.Sprintf("encoding conversion failed %s", i.charset))
	}

	e.Fill(e.Meta(), e.Header(), bytes)
	return nil
}

func (i *CharsetDecoder) Hook(event api.Event) api.Result {
	i.process(event)
	return i.productFunc(event)
}
