/*
Copyright 2022 Loggie Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package franz

import (
	"encoding/json"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"strings"
)

type Logger struct {
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
