package process

import (
	"bytes"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/source/filestream/config"
	define "github.com/loggie-io/loggie/pkg/source/filestream/process/define"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
	"github.com/loggie-io/loggie/pkg/util"
)

// LineTerminator is the option storing the line terminator characters
// Supported newline reference: https://en.wikipedia.org/wiki/Newline#Unicode
type LineTerminator uint8

const (
	// InvalidTerminator is the invalid terminator
	InvalidTerminator LineTerminator = iota
	// AutoLineTerminator accepts both LF and CR+LF
	AutoLineTerminator
	// LineFeed is the unicode char LF
	LineFeed
	// VerticalTab is the unicode char VT
	VerticalTab
	// FormFeed is the unicode char FF
	FormFeed
	// CarriageReturn is the unicode char CR
	CarriageReturn
	// CarriageReturnLineFeed is the unicode chars CR+LF
	CarriageReturnLineFeed
	// NextLine is the unicode char NEL
	NextLine
	// LineSeparator is the unicode char LS
	LineSeparator
	// ParagraphSeparator is the unicode char PS
	ParagraphSeparator
	// NullTerminator
	NullTerminator
)

var (
	lineTerminators = map[string]LineTerminator{
		"auto":                      AutoLineTerminator,
		"line_feed":                 LineFeed,
		"vertical_tab":              VerticalTab,
		"form_feed":                 FormFeed,
		"carriage_return":           CarriageReturn,
		"carriage_return_line_feed": CarriageReturnLineFeed,
		"next_line":                 NextLine,
		"line_separator":            LineSeparator,
		"paragraph_separator":       ParagraphSeparator,
		"null_terminator":           NullTerminator,
	}

	lineTerminatorCharacters = map[LineTerminator][]byte{
		AutoLineTerminator:     {'\u000A'},
		LineFeed:               {'\u000A'},
		VerticalTab:            {'\u000B'},
		FormFeed:               {'\u000C'},
		CarriageReturn:         {'\u000D'},
		CarriageReturnLineFeed: []byte("\u000D\u000A"),
		NextLine:               {'\u0085'},
		LineSeparator:          []byte("\u2028"),
		ParagraphSeparator:     []byte("\u2029"),
		NullTerminator:         {'\u0000'},
	}
)

// LineProcessor 多行处理器模块，用来在发送之前的多行处理
type LineProcessor struct {
	progress           storage.CollectProgress
	multiProcessConfig *config.LineProcessConfig
	next               define.CollectProcessor
	split              []byte
	epoch              *pipeline.Epoch
	backLogBuffer      []byte
}

// NewLineProcessor 新建多行采集处理器
func NewLineProcessor(progress storage.CollectProgress, multiProcessConfig *config.LineProcessConfig) define.CollectProcessor {

	if !*multiProcessConfig.Enable {
		return nil
	}

	termType, ok := lineTerminators[multiProcessConfig.Terminator]
	if ok == false {
		termType = lineTerminators["auto"]
		log.Error("terminator is not exist:%s", multiProcessConfig.Charset)
	}

	splitByte, err := util.Encode(multiProcessConfig.Charset, lineTerminatorCharacters[termType])

	if err != nil {
		log.Error("util Encode error:%s", err)
	}

	return &LineProcessor{
		progress:           progress,
		multiProcessConfig: multiProcessConfig,
		split:              splitByte,
	}
}

func (m *LineProcessor) Next(processor define.CollectProcessor) {
	m.next = processor
}

func (m *LineProcessor) OnError(err error) {
	m.next.OnError(err)
}

// OnProcess 将内容拆分成为多行发给Decoder 模块
// 並且要在这个模块的最后一个节点打标
// 用来做持久化落地
func (m *LineProcessor) OnProcess(ctx define.Context) {
	ctx.SetEndLineLength(uint8(len(m.split)))
	if !*m.multiProcessConfig.Enable {
		m.next.OnProcess(ctx)
		return
	}

	var buf []byte

	// 检查缓冲区是否有未发送的数据,有的话需要将buf 拼接到缓冲区域

	if len(m.backLogBuffer) != 0 {
		buf = append(m.backLogBuffer, ctx.GetBuf()...)
	} else {
		buf = ctx.GetBuf()
	}
	bufLen := len(buf)

	//进行多行切割, 遵循编码规则, if else 不能嵌套超过三层
	var off int
	for {
		idx := bytes.Index(buf[off:], m.split)

		// 找到了换行符
		if idx >= 0 {
			endLen := off + idx
			data := buf[off:endLen]
			if len(data) == 0 {
				return
			}

			// 是最后块了，要打标记
			if endLen == (bufLen - len(m.split)) {
				ctx.MarkLastLine()
			}
			m.progress.AddCollectLine(1)
			ctx.SetBuf(data)
			m.next.OnProcess(ctx)
			off += idx + len(m.split)
			continue

		}

		// 找不到换行符
		m.backLogBuffer = append(m.backLogBuffer, buf[off:]...)

		break
	}

	backLogBufferLength := uint64(len(m.backLogBuffer))
	// 缓冲区中的字节数超过了最大值，那么就要发送了
	if backLogBufferLength > m.multiProcessConfig.MaxBackLogSize {
		ctx.SetBuf(m.backLogBuffer)
		ctx.MarkLastLine()
		m.backLogBuffer = []byte{}
		m.next.OnProcess(ctx)
	}

}

func (m *LineProcessor) OnComplete(ctx define.Context) {
	if len(m.backLogBuffer) != 0 {
		if len(ctx.GetBuf()) != 0 {
			m.backLogBuffer = append(m.backLogBuffer, ctx.GetBuf()...)
		}
		ctx.SetBuf(m.backLogBuffer)
		m.next.OnComplete(ctx)
		return
	}
	m.next.OnComplete(ctx)
}
