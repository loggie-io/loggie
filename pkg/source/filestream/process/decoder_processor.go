package process

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/filestream/config"
	define "github.com/loggie-io/loggie/pkg/source/filestream/process/define"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
	"github.com/loggie-io/loggie/pkg/util"
	"golang.org/x/text/encoding"
)

type DecoderProcessor struct {
	progress      storage.CollectProgress
	decoderConfig *config.DecoderProcessConfig
	decoder       *encoding.Decoder
	next          define.CollectProcessor
	backLogBuffer []byte
}

func NewDecoderProcessor(progress storage.CollectProgress, decoderConfig *config.DecoderProcessConfig) define.CollectProcessor {

	_, ok := util.AllEncodings[decoderConfig.Charset]
	if ok == false {
		decoderConfig.Charset = "utf-8"
		log.Error("decoderConfig.Charset : %s ", decoderConfig.Charset)
	}

	return &DecoderProcessor{
		progress:      progress,
		decoderConfig: decoderConfig,
		decoder:       util.AllEncodings[decoderConfig.Charset].NewDecoder(),
	}
}

func (d *DecoderProcessor) Next(processor define.CollectProcessor) {
	d.next = processor
}

func (d *DecoderProcessor) OnError(error) {

}

// OnProcess 多编码处理,将不同的编码转化为utf-8
func (d *DecoderProcessor) OnProcess(ctx define.Context) {
	defer d.next.OnProcess(ctx)
	if d.decoderConfig.Charset == "utf-8" {
		return
	}

	d.decode(ctx)
	return
}

func (d *DecoderProcessor) decode(ctx define.Context) {
	buf, err := d.decoder.Bytes(ctx.GetBuf())

	if err != nil {
		log.Error("decoder error:%s", d.decoderConfig.Charset)
		return
	}

	ctx.SetBuf(buf)
}

func (d *DecoderProcessor) OnComplete(ctx define.Context) {
	if len(ctx.GetBuf()) != 0 {
		d.decode(ctx)
	}
	d.next.OnComplete(ctx)
}
