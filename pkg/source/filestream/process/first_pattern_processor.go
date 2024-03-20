package process

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/filestream/config"
	define "github.com/loggie-io/loggie/pkg/source/filestream/process/define"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
	"regexp"
)

type FirstPatternProcessor struct {
	next          define.CollectProcessor
	progress      storage.CollectProgress
	patternConfig *config.MultiProcessConfig
	matcher       *regexp.Regexp
	backLogBuffer []byte
}

func NewFirstPatternProcessor(progress storage.CollectProgress, patternConfig *config.MultiProcessConfig) define.CollectProcessor {

	if !*patternConfig.Enable {
		return nil
	}

	if patternConfig.FirstPattern == "" {
		return nil
	}

	matcher, err := regexp.Compile(patternConfig.FirstPattern)
	if err != nil {
		log.Error("regexp.Compile error:%s", err)
		return nil
	}

	return &FirstPatternProcessor{
		patternConfig: patternConfig,
		progress:      progress,
		matcher:       matcher,
	}
}

func (f *FirstPatternProcessor) Next(processor define.CollectProcessor) {
	f.next = processor
}

func (f *FirstPatternProcessor) OnError(err error) {
	f.next.OnError(err)
}

func (f *FirstPatternProcessor) OnProcess(ctx define.Context) {
	if !*f.patternConfig.Enable {
		f.next.OnProcess(ctx)
		return
	}

	if len(ctx.GetBuf()) == 0 {
		return
	}

	var buf []byte
	buf = ctx.GetBuf()
	//// 检查缓冲区是否有未发送的数据,有的话需要将buf 拼接到缓冲区域
	//if len(f.backLogBuffer) != 0 {
	//	buf = append(ctx.GetBuf(), f.backLogBuffer...)
	//}
	//
	//if len(buf) == 0 {
	//	return
	//}

	// 行首匹配
	ret := f.matcher.Match(ctx.GetBuf())

	// 如果说匹配到行首
	if ret {
		if f.backLogBuffer != nil {
			ctx.SetBuf(f.backLogBuffer)
			f.next.OnProcess(ctx)
			f.backLogBuffer = buf
			return
		}
		f.backLogBuffer = append(f.backLogBuffer, buf...)
		return
	}

	// 没匹配到行首
	f.backLogBuffer = append(f.backLogBuffer, buf...)

	if uint64(len(f.backLogBuffer)) >= f.patternConfig.MaxBackLogSize {
		ctx.SetBuf(f.backLogBuffer)
		f.next.OnProcess(ctx)
		f.backLogBuffer = []byte{}
		return
	}

	return

}

func (f *FirstPatternProcessor) OnComplete(ctx define.Context) {
	if f.backLogBuffer != nil {
		if len(ctx.GetBuf()) != 0 {
			f.backLogBuffer = append(f.backLogBuffer, ctx.GetBuf()...)
		}
		ctx.SetBuf(f.backLogBuffer)
		f.next.OnComplete(ctx)
		return
	}
	f.next.OnComplete(ctx)
}
