package process

import (
	"bytes"
	"time"

	"github.com/loggie-io/loggie/pkg/source/file"
)

type LineConfig struct {
	MaxBytes          int    `yaml:"maxBytes,omitempty" default:"131072" validate:"gt=0"` // default 128k
	OverBytesStrategy string `yaml:"overBytesStrategy"`                                   // newLine、split
}

func init() {
	file.RegisterProcessor(makeLine)
}

func makeLine(config file.ReaderConfig) file.Processor {
	return &LineProcessor{}
}

type LineProcessor struct {
	config  LineConfig
	newLine []byte
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
		index := int64(bytes.IndexByte(readBuffer[processed:], '\n'))
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
			job.ProductEvent(endOffset, now, readBuffer[processed:index])
		}
		processed = index + 1
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
