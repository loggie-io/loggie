package process

import (
	"io"
	"time"

	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/file"
)

func init() {
	file.RegisterProcessor(makeLastLine)
}

func makeLastLine(config file.ReaderConfig) file.Processor {
	return &LastLineProcessor{
		inactiveTimeout: config.InactiveTimeout,
	}
}

type LastLineProcessor struct {
	inactiveTimeout time.Duration
}

func (llp *LastLineProcessor) Order() int {
	return 100
}

func (llp *LastLineProcessor) Code() string {
	return "lastLine"
}

func (llp *LastLineProcessor) Process(processorChain file.ProcessChain, ctx *file.JobCollectContext) {
	ctx.BacklogBuffer = ctx.BacklogBuffer[:0]
	// see LoopProcessor.Process()
	processorChain.Process(ctx)
	// check last line
	l := len(ctx.BacklogBuffer)
	if l <= 0 {
		return
	}
	job := ctx.Job
	// When it is necessary to back off the offset, check whether it is inactive to collect the last line
	isLastLineSend := false
	if ctx.IsEOF && !ctx.WasSend {
		if time.Since(job.LastActiveTime) >= llp.inactiveTimeout {
			// Send "last line"
			endOffset := ctx.LastOffset
			job.ProductEvent(endOffset, time.Now(), ctx.BacklogBuffer)
			job.LastActiveTime = time.Now()
			isLastLineSend = true
			// Ignore the /n that may be written next.
			// Because the "last line" of the collection thinks that either it will not be written later,
			// or it will write /n first, and then write the content of the next line,
			// it is necessary to seek a position later to ignore the /n that may be written
			_, err := job.File().Seek(int64(len(job.GetEncodeLineEnd())), io.SeekCurrent)
			if err != nil {
				log.Error("can't set offset, file(name:%s) seek error: %v", ctx.Filename, err)
			}
		} else {
			// Enable the job to escape and collect the last line
			job.EofCount = 0
		}
	}
	// Fallback accumulated buffer offset
	if !isLastLineSend {
		backwardOffset := int64(-l)
		_, err := job.File().Seek(backwardOffset, io.SeekCurrent)
		if err != nil {
			log.Error("can't set offset(%d), file(name:%s) seek error: %v", backwardOffset, ctx.Filename, err)
		}
		return
	}
}
