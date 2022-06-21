package file

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/log"
	"strings"
	"sync"
)

// LineTerminator is the option storing the line terminator characters
// Supported newline reference: https://en.wikipedia.org/wiki/Newline#Unicode
type LineTerminator uint8

const (
	Customize = "customize"
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
		AutoLineTerminator:     []byte{'\u000A'},
		LineFeed:               []byte{'\u000A'},
		VerticalTab:            []byte{'\u000B'},
		FormFeed:               []byte{'\u000C'},
		CarriageReturn:         []byte{'\u000D'},
		CarriageReturnLineFeed: []byte("\u000D\u000A"),
		NextLine:               []byte{'\u0085'},
		LineSeparator:          []byte("\u2028"),
		ParagraphSeparator:     []byte("\u2029"),
		NullTerminator:         []byte{'\u0000'},
	}
)

type LineEndingsValue struct {
	value       []byte
	encodeValue []byte
}

type LineEndings struct {
	mutex      sync.RWMutex
	endLineMap map[string]*LineEndingsValue
}

var globalLineEnd LineEndings

func (end *LineEndings) Init() {
	end.mutex.Lock()
	defer end.mutex.Unlock()
	if end.endLineMap == nil {
		end.endLineMap = make(map[string]*LineEndingsValue)
	}
}

func (end *LineEndings) AddLineEnd(pipelineName string, sourceName string, lineEndValue *LineEndValue, charset string) error {
	end.mutex.Lock()
	defer end.mutex.Unlock()
	lineType, ok := lineTerminators[lineEndValue.LineType]
	if ok == false && lineEndValue.LineType != Customize {
		lineType = AutoLineTerminator
	}

	var key strings.Builder
	key.WriteString(pipelineName)
	key.WriteString(":")
	key.WriteString(sourceName)

	if lineEndValue.LineType == Customize {
		bytes, err := Encode(charset, []byte(lineEndValue.LineValue))
		if err != nil {
			log.Error("encode error:%s", err)
		}
		end.endLineMap[key.String()] = &LineEndingsValue{
			value:       []byte(lineEndValue.LineValue),
			encodeValue: bytes,
		}
		return nil
	}

	bytes, err := Encode(charset, lineTerminatorCharacters[lineType])
	if err != nil {
		log.Error("encode error:%s", err)
	}
	end.endLineMap[key.String()] = &LineEndingsValue{
		value:       lineTerminatorCharacters[lineType],
		encodeValue: bytes,
	}
	return nil
}

func (end *LineEndings) GetLineEnd(pipelineName string, sourceName string) []byte {
	end.mutex.RLock()
	defer end.mutex.RUnlock()
	var key strings.Builder
	key.WriteString(pipelineName)
	key.WriteString(":")
	key.WriteString(sourceName)
	value, ok := end.endLineMap[key.String()]
	if ok == false {
		return []byte("\n")
	}
	return value.value
}

func (end *LineEndings) GetEncodeLineEnd(pipelineName string, sourceName string) []byte {
	end.mutex.RLock()
	defer end.mutex.RUnlock()
	var key strings.Builder
	key.WriteString(pipelineName)
	key.WriteString(":")
	key.WriteString(sourceName)
	value, ok := end.endLineMap[key.String()]
	if ok == false {
		return []byte("\n")
	}
	return value.encodeValue
}

func (end *LineEndings) RemoveLineEnd(pipelineName string, sourceName string) {
	end.mutex.Lock()
	defer end.mutex.Unlock()
	key := fmt.Sprintf("%s:%s", pipelineName, sourceName)
	delete(end.endLineMap, key)
}
