package process

import (
	"bytes"
	"time"

	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/file"
)

func init() {
	file.RegisterProcessor(makeLine)
}

func makeLine(config file.ReaderConfig) file.Processor {
	return &LineProcessor{
		maxSingleLineBytes: config.MaxSingleLineBytes,
	}
}

type LineProcessor struct {
	maxSingleLineBytes int64
}

func (lp *LineProcessor) Order() int {
	return 500
}

func (lp *LineProcessor) Code() string {
	return "line"
}

func (lp *LineProcessor) Process(processorChain file.ProcessChain, ctx *file.JobCollectContext) {
	job := ctx.Job
	now := time.Now()
	readBuffer := ctx.ReadBuffer
	read := int64(len(readBuffer))
	processed := int64(0)
	for processed < read {
		index := int64(bytes.Index(readBuffer[processed:], job.GetEncodeLineEnd()))
		if index == -1 {
			break
		}
		index += processed

		endOffset := ctx.LastOffset + index
		if len(ctx.BacklogBuffer) != 0 {
			ctx.BacklogBuffer = append(ctx.BacklogBuffer, readBuffer[processed:index]...)
			job.ProductEvent(endOffset, now, ctx.BacklogBuffer)

			// Clean the backlog buffer after sending
			ctx.BacklogBuffer = ctx.BacklogBuffer[:0]
		} else {
			buffer := readBuffer[processed:index]
			if len(buffer) > 0 {
				job.ProductEvent(endOffset, now, buffer)
			}
		}
		processed = index + int64(len(job.GetEncodeLineEnd()))
	}
	ctx.LastOffset += read
	ctx.WasSend = processed != 0
	// The remaining bytes read are added to the backlog buffer
	if processed < read {
		// check whether it is too long to avoid bursting the memory
		// if the limit is exceeded, the remaining content on the line will be ignored.
		if int64(len(ctx.BacklogBuffer)) <= lp.maxSingleLineBytes {
			ctx.BacklogBuffer = append(ctx.BacklogBuffer, readBuffer[processed:]...)
		} else {
			log.Warn("[%s]The length of the backlog buffer exceeds the limit(%s kb), the remaining content on the line will be ignored with offset(%d).", job.FileName(), lp.maxSingleLineBytes/1024, ctx.LastOffset-read)
		}
	}
}
