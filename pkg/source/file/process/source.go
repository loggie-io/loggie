package process

import (
	"errors"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/file"
	"io"
)

func init() {
	file.RegisterProcessor(makeSource)
}

func makeSource(config file.ReaderConfig) file.Processor {
	return &SourceProcessor{
		readBufferSize: config.ReadBufferSize,
	}
}

type SourceProcessor struct {
	readBufferSize int
}

func (sp *SourceProcessor) Order() int {
	return 300
}

func (sp *SourceProcessor) Code() string {
	return "source"
}

func (sp *SourceProcessor) Process(processorChain file.ProcessChain, ctx *file.JobCollectContext) {
	job := ctx.Job
	ctx.ReadBuffer = ctx.ReadBuffer[:sp.readBufferSize]
	l, err := job.File().Read(ctx.ReadBuffer)
	if errors.Is(err, io.EOF) || l == 0 {
		ctx.IsEOF = true
		job.EofCount++
		return
	}
	if err != nil {
		ctx.IsEOF = true
		log.Error("file(name:%s) read fail: %v", ctx.Filename, err)
		return
	}
	read := int64(l)
	ctx.ReadBuffer = ctx.ReadBuffer[:read]

	// see lineProcessor.Process
	processorChain.Process(ctx)
}
