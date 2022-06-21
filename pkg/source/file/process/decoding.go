package process

// refers to https://github.com/elastic/beats/blob/9da790294d230475cfbbec5c969fb585c9834017/libbeat/reader/readfile/encoding/encoding.go
import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/file"
)

type DecodingProcess struct {
	charset string
}

func init() {
	file.RegisterProcessor(makeEncoding)
}

func makeEncoding(config file.ReaderConfig) file.Processor {
	return &DecodingProcess{
		charset: config.Charset,
	}
}

func (en *DecodingProcess) Order() int {
	return 400
}

func (en *DecodingProcess) Code() string {
	return "decoding"
}

func (en *DecodingProcess) Process(processorChain file.ProcessChain, ctx *file.JobCollectContext) {
	defer func() {
		processorChain.Process(ctx)
	}()

	if en.charset == "utf-8" {
		return
	}

	codec, ok := file.AllEncodings[en.charset]

	if !ok {
		log.Warn(fmt.Sprintf("unknown Charset('%v')", en.charset))
		return
	}
	bytes, err := codec.NewDecoder().Bytes(ctx.ReadBuffer)

	if err != nil {
		log.Warn(fmt.Sprintf("failed to encode %v into %v", ctx, en.charset))
		return
	}

	ctx.ReadBuffer = ctx.ReadBuffer[:0]
	ctx.ReadBuffer = append(ctx.ReadBuffer, bytes[0:]...)
}
