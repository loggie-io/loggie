package process

import (
	"github.com/loggie-io/loggie/pkg/source/file"
	"time"
)

func init() {
	file.RegisterProcessor(makeLoop)
}

func makeLoop(config file.ReaderConfig) file.Processor {
	return &LoopProcessor{
		maxContinueRead:        config.MaxContinueRead,
		maxContinueReadTimeout: config.MaxContinueReadTimeout,
	}
}

type LoopProcessor struct {
	maxContinueRead        int
	maxContinueReadTimeout time.Duration

	continueRead  int
	startReadTime time.Time
}

func (bp *LoopProcessor) Order() int {
	return 200
}

func (bp *LoopProcessor) Code() string {
	return "loop"
}

func (bp *LoopProcessor) Process(processorChain file.ProcessChain, ctx *file.JobCollectContext) {
	bp.startReadTime = time.Now()
	bp.continueRead = 0
	for {
		// see SourceProcessor.Process
		processorChain.Process(ctx)

		if ctx.IsEOF {
			break
		}

		if ctx.WasSend {
			bp.continueRead++
			// According to the number of batches 2048, a maximum of one batch can be read,
			// and a single event is calculated according to 512 bytes, that is, the maximum reading is 1mb ,maxContinueRead = 16 by default
			// SSD recommends that maxContinueRead be increased by 3 ~ 5x
			if bp.continueRead > bp.maxContinueRead {
				break
			}
			if time.Since(bp.startReadTime) > bp.maxContinueReadTimeout {
				break
			}
		}
	}

	// send event, reset job eof count
	if ctx.WasSend {
		ctx.Job.EofCount = 0
		ctx.Job.LastActiveTime = time.Now()
	}
}
