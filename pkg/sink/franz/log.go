package franz

import (
	"encoding/json"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"strings"
)

type Logger struct {
	//// Level returns the log level to log at.
	////
	//// Implementations can change their log level on the fly, but this
	//// function must be safe to call concurrently.
	//Level() kgo.LogLevel
	//
	//// Log logs a message with key, value pair arguments for the given log
	//// level. Keys are always strings, while values can be any type.
	////
	//// This must be safe to call concurrently.
	//Log(level LogLevel, msg string, keyvals ...interface{})
}

func (*Logger) Level() kgo.LogLevel {
	switch log.Level() {
	case zerolog.DebugLevel:
		return kgo.LogLevelDebug
	case zerolog.InfoLevel:
		return kgo.LogLevelInfo
	case zerolog.WarnLevel:
		return kgo.LogLevelWarn
	case zerolog.ErrorLevel:
		return kgo.LogLevelError
	case zerolog.FatalLevel:
		return kgo.LogLevelError
	case zerolog.PanicLevel:
		return kgo.LogLevelError
	case zerolog.NoLevel:
		return kgo.LogLevelInfo
	case zerolog.Disabled:
		return kgo.LogLevelNone
	case zerolog.TraceLevel:
		return kgo.LogLevelDebug
	}
	return kgo.LogLevelDebug
}

func (l *Logger) Log(level kgo.LogLevel, msg string, keyvals ...interface{}) {
	var builder strings.Builder
	builder.WriteString(msg)
	ret, err := json.Marshal(keyvals)
	if err == nil {
		builder.Write(ret)
	}
	switch level {
	case kgo.LogLevelNone:
		return
	case kgo.LogLevelError:
		log.Error(builder.String())
		return
	case kgo.LogLevelWarn:
		log.Warn(builder.String())
	case kgo.LogLevelInfo:
		log.Info(builder.String())
	case kgo.LogLevelDebug:
		log.Debug(builder.String())
	}
	log.Debug(builder.String())
}
