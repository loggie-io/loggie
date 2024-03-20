package define

import "github.com/loggie-io/loggie/pkg/source/filestream/ack"

// Context process上下文,用来在process链条中传递
type Context interface {
	SetBuf([]byte)

	GetBuf() []byte

	SetPath(string)

	GetPath() string

	GetAckMark() ack.Mark

	SetAckMark(mark ack.Mark)

	SetEndLineLength(uint8)

	GetEndLineLength() uint8

	SetHash(uint64)

	GetHash() uint64

	MarkLastLine()

	IsLastLine() bool

	ResetMarkLastLine()
}
