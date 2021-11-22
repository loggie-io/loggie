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
	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"loggie.io/loggie/pkg/core/log/spi"
	"os"
	"path"
)

var (
	l          *zerolog.Logger
	AfterError spi.AfterError

	// log format for local development
	jsonFormat bool
	// global log output level
	logLevel string
	// FileLoggingEnabled makes the framework log to a file
	fileLoggingEnabled bool
	// Directory to log to to when filelogging is enabled
	directory string
	// Filename is the name of the logfile which will be placed inside the directory
	filename string
	// MaxSize the max size in MB of the logfile before it's rolled
	maxSize int
	// MaxBackups the max number of rolled files to keep
	maxBackups int
	// MaxAge the max age in days to keep a logfile
	maxAge int
	// TimeFormat log time format
	timeFormat string
)

func init() {
	flag.StringVar(&logLevel, "log.level", "info", "Global log output level")
	flag.BoolVar(&jsonFormat, "log.jsonFormat", true, "Parses the JSON log format")
	flag.BoolVar(&fileLoggingEnabled, "log.enableFile", false, "FileLoggingEnabled makes the framework log to a file")
	flag.StringVar(&directory, "log.directory", "/var/log", "Directory to log to to when log.enableFile is enabled")
	flag.StringVar(&filename, "log.filename", "loggie.log", "Filename is the name of the logfile which will be placed inside the directory")
	flag.IntVar(&maxSize, "log.maxSize", 1024, "Max size in MB of the logfile before it's rolled")
	flag.IntVar(&maxBackups, "log.maxBackups", 3, "Max number of rolled files to keep")
	flag.IntVar(&maxAge, "log.maxAge", 7, "Max age in days to keep a logfile")
	flag.StringVar(&timeFormat, "log.timeFormat", "2006-01-02 15:04:05", "TimeFormat log time format")
}

func InitLog() {

	var writers []io.Writer

	if jsonFormat {
		writers = append(writers, os.Stderr)
	} else {
		writers = append(writers, zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: timeFormat,
		})
	}
	if fileLoggingEnabled {
		writers = append(writers, newRollingFile())
	}

	mw := io.MultiWriter(writers...)

	zerolog.CallerSkipFrameCount = 3
	level, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		panic("set log level error, choose trace/debug/info/warn/error/fatal/panic")
	}
	zerolog.SetGlobalLevel(level)
	logger := zerolog.New(mw).With().Timestamp().Caller().Logger()

	l = &logger
}

func newRollingFile() io.Writer {
	if err := os.MkdirAll(directory, 0744); err != nil {
		panic("can't create log directory")
	}

	return &lumberjack.Logger{
		Filename:   path.Join(directory, filename),
		MaxBackups: maxBackups, // files
		MaxSize:    maxSize,    // megabytes
		MaxAge:     maxAge,     // days
	}
}

func Debug(format string, a ...interface{}) {
	if a == nil {
		l.Debug().Msg(format)
	} else {
		l.Debug().Msgf(format, a...)
	}
}

func Info(format string, a ...interface{}) {
	if a == nil {
		l.Info().Msg(format)
	} else {
		l.Info().Msgf(format, a...)
	}
}

func Warn(format string, a ...interface{}) {
	if a == nil {
		l.Warn().Msg(format)
	} else {
		l.Warn().Msgf(format, a...)
	}
}

func Error(format string, a ...interface{}) {
	defer afterErrorOpt(format, a...)
	if a == nil {
		l.Error().Msg(format)
	} else {
		l.Error().Msgf(format, a...)
	}
}

func Panic(format string, a ...interface{}) {
	defer afterErrorOpt(format, a...)
	if a == nil {
		l.Panic().Msg(format)
	} else {
		l.Panic().Msgf(format, a...)
	}
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
