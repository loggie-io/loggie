package file

import (
	"errors"
	"io"
	"sort"
	"strings"

	"github.com/loggie-io/loggie/pkg/core/log"
)

type JobCollectContext struct {
	Job           *Job
	Filename      string
	LastOffset    int64
	BacklogBuffer []byte
	ReadBuffer    []byte

	// runtime property
	WasSend bool
	IsEOF   bool
}

func NewJobCollectContextAndValidate(job *Job, readBuffer, backlogBuffer []byte) (*JobCollectContext, error) {
	lastOffset, err := validateJob(job)
	if err != nil {
		return nil, err
	}
	return &JobCollectContext{
		Job:           job,
		Filename:      job.filename,
		LastOffset:    lastOffset,
		BacklogBuffer: backlogBuffer,
		ReadBuffer:    readBuffer,
	}, nil
}

func validateJob(job *Job) (lastOffset int64, err error) {
	filename := job.filename
	status := job.status
	if status == JobStop {
		log.Info("Job(uid: %s) file(%s) status(%d) is stop, Job will be ignore", job.Uid(), filename, status)
		return 0, errors.New("Job is stop")
	}
	osFile := job.file
	if osFile == nil {
		log.Error("Job(uid: %s) file(%s) released,Job will be ignore", job.Uid(), filename)
		return 0, errors.New("Job file released")
	}
	lastOffset, err = osFile.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Error("can't get offset, file(name:%s) seek error, err: %v", filename, err)
		return 0, err
	}
	return lastOffset, nil
}

type ProcessChain interface {
	Process(ctx *JobCollectContext)
}

type abstractProcessChain struct {
	DoProcess func(ctx *JobCollectContext)
}

func (ap *abstractProcessChain) Process(ctx *JobCollectContext) {
	ap.DoProcess(ctx)
}

type Processor interface {
	Order() int
	Code() string
	Process(processorChain ProcessChain, ctx *JobCollectContext)
}

type sortableProcessor struct {
	processors []Processor
}

func newSortableProcessor() *sortableProcessor {
	return &sortableProcessor{
		processors: make([]Processor, 0),
	}
}

func (sp *sortableProcessor) Len() int {
	return len(sp.processors)
}

func (sp *sortableProcessor) Less(i, j int) bool {
	pi := sp.processors[i]
	pj := sp.processors[j]
	return pi.Order() < pj.Order()
}

func (sp *sortableProcessor) Swap(i, j int) {
	sp.processors[i], sp.processors[j] = sp.processors[j], sp.processors[i]
}

func (sp *sortableProcessor) Sort() {
	sort.SliceStable(sp.processors, func(i, j int) bool {
		return sp.Less(i, j)
	})
}

func (sp *sortableProcessor) Append(processor Processor) {
	sp.processors = append(sp.processors, processor)
}

func (sp *sortableProcessor) Processors() []Processor {
	return sp.processors
}

func NewProcessChain(config ReaderConfig) ProcessChain {
	l := len(processFactories)
	if l == 0 {
		return nil
	}
	sp := newSortableProcessor()
	for _, factory := range processFactories {
		sp.Append(factory(config))
	}
	sp.Sort()
	processors := sp.Processors()
	pl := len(processors)
	var processChainName strings.Builder
	processChainName.WriteString("start->")
	last := &abstractProcessChain{
		DoProcess: func(ctx *JobCollectContext) {
			// do nothing
		},
	}
	for i := 0; i < pl; i++ {
		// Reverse order
		tempProcessor := processors[pl-1-i]
		next := last
		last = &abstractProcessChain{
			DoProcess: func(ctx *JobCollectContext) {
				tempProcessor.Process(next, ctx)
			},
		}

		processChainName.WriteString(processors[i].Code())
		processChainName.WriteString("->")
	}
	processChainName.WriteString("end")
	log.Info("process chain: %s", processChainName.String())
	return last
}

var processFactories = make([]ProcessFactory, 0)

type ProcessFactory func(config ReaderConfig) Processor

func RegisterProcessor(factory ProcessFactory) {
	processFactories = append(processFactories, factory)
}
