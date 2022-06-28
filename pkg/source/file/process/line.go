package process

import (
	"bytes"
	"time"

	"github.com/loggie-io/loggie/pkg/source/file"
)

func init() {
	file.RegisterProcessor(makeLine)
}

func makeLine(config file.ReaderConfig) file.Processor {
	return &LineProcessor{}
}

type LineProcessor struct {
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
		ctx.BacklogBuffer = append(ctx.BacklogBuffer, readBuffer[processed:]...)

		// TODO check whether it is too long to avoid bursting the memory
		//if len(backlogBuffer)>max_bytes{
		//	log.Error
		//	break
		//}
	}
}
