package stream

import (
	"github.com/loggie-io/loggie/pkg/source/filestream/ack"
	"github.com/loggie-io/loggie/pkg/source/filestream/process/define"
)

type Context struct {
	path          string
	buf           []byte
	endLineLength uint8
	mark          ack.Mark
	hash          uint64
	last          bool
}

func NewStreamContext() define.Context {
	return &Context{}
}

func (s *Context) SetBuf(buf []byte) {
	s.buf = buf
}

func (s *Context) GetBuf() []byte {
	return s.buf
}

func (s *Context) SetPath(path string) {
	s.path = path
}

func (s *Context) GetPath() string {
	return s.path
}

func (s *Context) SetEndLineLength(length uint8) {
	s.endLineLength = length
}

func (s *Context) GetEndLineLength() uint8 {
	return s.endLineLength
}

func (s *Context) SetAckMark(mark ack.Mark) {
	s.mark = mark
}

func (s *Context) GetAckMark() ack.Mark {
	return s.mark
}

func (s *Context) SetHash(hash uint64) {
	s.hash = hash
}

func (s *Context) GetHash() uint64 {
	return s.hash
}

func (s *Context) MarkLastLine() {
	s.last = true
}

func (s *Context) ResetMarkLastLine() {
	s.last = false
}

func (s *Context) IsLastLine() bool {
	return s.last
}
