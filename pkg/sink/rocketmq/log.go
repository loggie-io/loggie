package rocketmq

import (
	"github.com/loggie-io/loggie/pkg/core/log"
	"strings"
)

type rocketMQLogWrapper struct {
}

// newLoggieLogWrapper returns a new logger that wraps the loggie logger, witch implements the rocketmq logger interface.
// it aims to make the rocketmq logger compatible with loggie to avid rocketmq using its own log format.
func newLoggieLogWrapper() *rocketMQLogWrapper {
	return &rocketMQLogWrapper{}
}

func (r rocketMQLogWrapper) Debug(msg string, fields map[string]interface{}) {
	log.Debug("rocketmq sink client: %s, %v", msg, fields)
}

func (r rocketMQLogWrapper) Info(msg string, fields map[string]interface{}) {
	log.Info("rocketmq sink client: %s, %v", msg, fields)
}

func (r rocketMQLogWrapper) Warning(msg string, fields map[string]interface{}) {
	log.Warn("rocketmq sink client: %s, %v", msg, fields)
}

func (r rocketMQLogWrapper) Error(msg string, fields map[string]interface{}) {
	log.Error("rocketmq sink client: %s, %v", msg, fields)
}

func (r rocketMQLogWrapper) Fatal(msg string, fields map[string]interface{}) {
	log.Fatal("rocketmq sink client: %s, %v", msg, fields)
}

func (r rocketMQLogWrapper) Level(level string) {
	switch strings.ToLower(level) {
	case "debug":
	case "warn":
	case "error":
	case "fatal":
	default:
	}
}

func (r rocketMQLogWrapper) OutputPath(path string) (err error) {
	return nil
}
