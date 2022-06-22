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

package json_decode

import (
	"errors"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/source"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/encoding/unicode"
)

const Type = "iconv"

var AllEncodings = map[string]encoding.Encoding{
	// default
	"nop":   encoding.Nop,
	"plain": encoding.Nop,
	"utf-8": encoding.Nop,

	// simplified chinese
	"gbk": simplifiedchinese.GBK, // shadow htmlindex using 'GB10830' for GBK

	// traditional chinese
	"big5": traditionalchinese.Big5,

	// japanese
	"euc-jp":     japanese.EUCJP,
	"iso2022-jp": japanese.ISO2022JP,
	"shift-jis":  japanese.ShiftJIS,

	// korean
	"euc-kr": korean.EUCKR,

	// 8bit charmap encodings
	"iso8859-6e": charmap.ISO8859_6E,
	"iso8859-6i": charmap.ISO8859_6I,
	"iso8859-8e": charmap.ISO8859_8E,
	"iso8859-8i": charmap.ISO8859_8I,

	"iso8859-1":  charmap.ISO8859_1,  // latin-1
	"iso8859-2":  charmap.ISO8859_2,  // latin-2
	"iso8859-3":  charmap.ISO8859_3,  // latin-3
	"iso8859-4":  charmap.ISO8859_4,  // latin-4
	"iso8859-5":  charmap.ISO8859_5,  // latin/cyrillic
	"iso8859-6":  charmap.ISO8859_6,  // latin/arabic
	"iso8859-7":  charmap.ISO8859_7,  // latin/greek
	"iso8859-8":  charmap.ISO8859_8,  // latin/hebrew
	"iso8859-9":  charmap.ISO8859_9,  // latin-5
	"iso8859-10": charmap.ISO8859_10, // latin-6
	"iso8859-13": charmap.ISO8859_13, // latin-7
	"iso8859-14": charmap.ISO8859_14, // latin-8
	"iso8859-15": charmap.ISO8859_15, // latin-9
	"iso8859-16": charmap.ISO8859_16, // latin-10

	// ibm codepages
	"cp437":       charmap.CodePage437,
	"cp850":       charmap.CodePage850,
	"cp852":       charmap.CodePage852,
	"cp855":       charmap.CodePage855,
	"cp858":       charmap.CodePage858,
	"cp860":       charmap.CodePage860,
	"cp862":       charmap.CodePage862,
	"cp863":       charmap.CodePage863,
	"cp865":       charmap.CodePage865,
	"cp866":       charmap.CodePage866,
	"ebcdic-037":  charmap.CodePage037,
	"ebcdic-1040": charmap.CodePage1140,
	"ebcdic-1047": charmap.CodePage1047,

	// cyrillic
	"koi8r": charmap.KOI8R,
	"koi8u": charmap.KOI8U,

	// macintosh
	"macintosh":          charmap.Macintosh,
	"macintosh-cyrillic": charmap.MacintoshCyrillic,

	// windows
	"windows1250": charmap.Windows1250, // central and eastern european
	"windows1251": charmap.Windows1251, // russian, serbian cyrillic
	"windows1252": charmap.Windows1252, // legacy
	"windows1253": charmap.Windows1253, // modern greek
	"windows1254": charmap.Windows1254, // turkish
	"windows1255": charmap.Windows1255, // hebrew
	"windows1256": charmap.Windows1256, // arabic
	"windows1257": charmap.Windows1257, // estonian, latvian, lithuanian
	"windows1258": charmap.Windows1258, // vietnamese
	"windows874":  charmap.Windows874,

	// utf16 bom codecs
	"utf-16be-bom": unicode.UTF16(unicode.BigEndian, unicode.UseBOM),
	"utf-16le-bom": unicode.UTF16(unicode.LittleEndian, unicode.UseBOM),
}

func init() {
	pipeline.Register(api.INTERCEPTOR, Type, makeInterceptor)
}

func makeInterceptor(info pipeline.Info) api.Component {
	return &Interceptor{
		config: &Config{},
	}
}

type Interceptor struct {
	name   string
	config *Config
}

func (i *Interceptor) Config() interface{} {
	return i.config
}

func (i *Interceptor) Category() api.Category {
	return api.INTERCEPTOR
}

func (i *Interceptor) Type() api.Type {
	return Type
}

func (i *Interceptor) String() string {
	return fmt.Sprintf("%s/%s", i.Category(), i.Type())
}

func (i *Interceptor) Init(context api.Context) error {
	i.name = context.Name()
	return nil
}

func (i *Interceptor) Start() error {
	return nil
}

func (i *Interceptor) Stop() {

}

func (i *Interceptor) Intercept(invoker source.Invoker, invocation source.Invocation) api.Result {
	e := invocation.Event
	if err := i.process(e); err != nil {
		log.Error("encode event %s error: %v", e.String(), err)
	}
	return invoker.Invoke(invocation)
}

func (i *Interceptor) process(e api.Event) error {
	if i.config.Charset == "utf-8" {
		return nil
	}

	codec, ok := AllEncodings[i.config.Charset]

	if !ok {
		log.Warn("unknown Charset('%v')", i.config.Charset)
		return errors.New(fmt.Sprintf("unknown Charset('%v')", i.config.Charset))
	}

	bytes, err := codec.NewDecoder().Bytes(e.Body())

	if err != nil {
		log.Warn("failed to iconv  into %v", i.config.Charset)
		return errors.New(fmt.Sprintf("failed to encode  into %v", i.config.Charset))
	}

	e.Fill(e.Meta(), e.Header(), bytes)
	return nil
}
