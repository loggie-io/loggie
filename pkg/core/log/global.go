/*
Copyright 2021 Loggie Authors

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

package log

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/loggie-io/loggie/pkg/core/log/spi"
)

var (
	defaultLogger *Logger
	AfterError    spi.AfterError
	gLoggerConfig = &LoggerConfig{}
)

func init() {
	flag.StringVar(&gLoggerConfig.Level, "log.level", "info", "Global log output level")
	flag.BoolVar(&gLoggerConfig.JsonFormat, "log.jsonFormat", true, "Parses the JSON log format")
	flag.BoolVar(&gLoggerConfig.EnableStdout, "log.enableStdout", true, "EnableStdout enable the log print to stdout")
	flag.BoolVar(&gLoggerConfig.EnableFile, "log.enableFile", false, "EnableFile makes the framework log to a file")
	flag.StringVar(&gLoggerConfig.Directory, "log.directory", "/var/log", "Directory to log to to when log.enableFile is enabled")
	flag.StringVar(&gLoggerConfig.Filename, "log.filename", "loggie.log", "Filename is the name of the logfile which will be placed inside the directory")
	flag.IntVar(&gLoggerConfig.MaxSize, "log.maxSize", 1024, "Max size in MB of the logfile before it's rolled")
	flag.IntVar(&gLoggerConfig.MaxBackups, "log.maxBackups", 3, "Max number of rolled files to keep")
	flag.IntVar(&gLoggerConfig.MaxAge, "log.maxAge", 7, "Max age in days to keep a logfile")
	flag.StringVar(&gLoggerConfig.TimeFormat, "log.timeFormat", "2006-01-02 15:04:05", "TimeFormat log time format")
	flag.IntVar(&gLoggerConfig.CallerSkipCount, "log.callerSkipCount", 4, "CallerSkipCount is the number of stack frames to skip to find the caller")
	flag.BoolVar(&gLoggerConfig.NoColor, "log.noColor", false, "NoColor disables the colorized output")
}

type LoggerConfig struct {
	Level           string `yaml:"level,omitempty"`
	JsonFormat      bool   `yaml:"jsonFormat,omitempty"`
	EnableStdout    bool   `yaml:"enableStdout,omitempty"`
	EnableFile      bool   `yaml:"enableFile,omitempty"`
	Directory       string `yaml:"directory,omitempty"`
	Filename        string `yaml:"filename,omitempty"`
	MaxSize         int    `yaml:"maxSize,omitempty"`
	MaxBackups      int    `yaml:"maxBackups,omitempty"`
	MaxAge          int    `yaml:"maxAge,omitempty"`
	TimeFormat      string `yaml:"timeFormat,omitempty"`
	CallerSkipCount int    `yaml:"callerSkipCount,omitempty"`
	NoColor         bool   `yaml:"noColor,omitempty"`
}

type Logger struct {
	l *zerolog.Logger
}

func InitDefaultLogger() {
	logger := NewLogger(gLoggerConfig)
	defaultLogger = logger
}

func NewLogger(config *LoggerConfig) *Logger {
	var writers []io.Writer

	if config.EnableStdout {
		if config.JsonFormat {
			writers = append(writers, os.Stderr)
		} else {
			writers = append(writers, zerolog.ConsoleWriter{
				Out:        os.Stderr,
				TimeFormat: config.TimeFormat,
				NoColor:    config.NoColor,
			})
		}
	}

	if config.EnableFile {
		writers = append(writers, newRollingFile(config.Directory, config.Filename, config.MaxBackups, config.MaxSize, config.MaxAge))
	}

	mw := io.MultiWriter(writers...)

	zerolog.CallerSkipFrameCount = config.CallerSkipCount
	level, err := zerolog.ParseLevel(config.Level)
	if err != nil {
		panic("set log level error, choose trace/debug/info/warn/error/fatal/panic")
	}
	multi := zerolog.MultiLevelWriter(mw)
	logger := zerolog.New(multi).Level(level).With().Timestamp().Caller().Logger()
	return &Logger{
		l: &logger,
	}
}

func newRollingFile(directory string, filename string, maxBackups int, maxSize int, maxAge int) io.Writer {
	if err := os.MkdirAll(directory, 0744); err != nil {
		panic(fmt.Sprintf("can't create log directory %s", directory))
	}

	return &lumberjack.Logger{
		Filename:   path.Join(directory, filename),
		MaxBackups: maxBackups, // files
		MaxSize:    maxSize,    // megabytes
		MaxAge:     maxAge,     // days
	}
}

func (logger *Logger) Debug(format string, a ...interface{}) {
	if a == nil {
		logger.l.Debug().Msg(format)
	} else {
		logger.l.Debug().Msgf(format, a...)
	}
}

func (logger *Logger) Info(format string, a ...interface{}) {
	if a == nil {
		logger.l.Info().Msg(format)
	} else {
		logger.l.Info().Msgf(format, a...)
	}
}

func (logger *Logger) Warn(format string, a ...interface{}) {
	if a == nil {
		logger.l.Warn().Msg(format)
	} else {
		logger.l.Warn().Msgf(format, a...)
	}
}

func (logger *Logger) Error(format string, a ...interface{}) {
	if a == nil {
		logger.l.Error().Msg(format)
	} else {
		logger.l.Error().Msgf(format, a...)
	}
}

func (logger *Logger) Panic(format string, a ...interface{}) {
	if a == nil {
		logger.l.Panic().Msg(format)
	} else {
		logger.l.Panic().Msgf(format, a...)
	}
}

func (logger *Logger) Fatal(format string, a ...interface{}) {
	if a == nil {
		logger.l.Fatal().Msg(format)
	} else {
		logger.l.Fatal().Msgf(format, a...)
	}
}

func (logger *Logger) RawJson(key string, raw []byte, format string, a ...interface{}) {
	if a == nil {
		logger.l.Log().RawJSON(key, raw).Msg(format)
	} else {
		logger.l.Log().RawJSON(key, raw).Msgf(format, a...)
	}
}

func Debug(format string, a ...interface{}) {
	defaultLogger.Debug(format, a...)
}

func Info(format string, a ...interface{}) {
	defaultLogger.Info(format, a...)
}

func Warn(format string, a ...interface{}) {
	defaultLogger.Warn(format, a...)
}

func Error(format string, a ...interface{}) {
	defer afterErrorOpt(format, a...)
	defaultLogger.Error(format, a...)
}

func Panic(format string, a ...interface{}) {
	defer afterErrorOpt(format, a...)
	defaultLogger.Panic(format, a...)
}

func Fatal(format string, a ...interface{}) {
	defaultLogger.Fatal(format, a...)
}

func afterErrorOpt(format string, a ...interface{}) {
	if AfterError == nil {
		return
	}
	var msg string
	if a == nil {
		msg = format
	} else {
		msg = fmt.Sprintf(format, a...)
	}
	AfterError(msg)
}
