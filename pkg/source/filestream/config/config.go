package config

import "time"

type Config struct {
	Inputs          []*InputConfig
	AckConfig       AckConfig              `yaml:"ack,omitempty"`
	Fields          map[string]interface{} `yaml:"fields,omitempty"`
	MaxWaitInterval time.Duration          `yaml:"monitorInterval,omitempty" default:"5s"`
	MaxWaitCount    int                    `yaml:"maxWaitCount,omitempty" default:"5"`
}

type AckConfig struct {
	Enable                *bool         `yaml:"enable,omitempty" default:"true"`
	MaintenanceInterval   time.Duration `yaml:"maintenanceInterval,omitempty" default:"20h"`
	MonitorExportInterval time.Duration `yaml:"monitorInterval,omitempty" default:"5s"`
}

type InputConfig struct {
	Source              string `yaml:"source,omitempty" default:"filestream"` // file, filestream, tcp
	FilePath            string `yaml:"filePath,omitempty"`                    // effect if Input is file or filestream
	Frame               string // singleline, multiline, syslog.RFCxxx, syslog.RFCyyy
	StreamName          string `yaml:"streamName,omitempty"` // effect if Source is filestream
	StreamProcessConfig ProcessConfig
	StreamMaxSize       int `yaml:"streamMaxSize,omitempty" default:"50"`
	StreamFileConfig    FileConfig
}

type FileConfig struct {
	HashSize    int64 `yaml:"hashSize,omitempty" default:"256"`
	MaxReadSize int64 `yaml:"maxReadSize,omitempty" default:"1048576"` //默认每次读1M
}

type ProcessConfig struct {
	StreamMultiProcessConfig        LineProcessConfig
	StreamDecoderProcessConfig      DecoderProcessConfig
	StreamFirstPatternProcessConfig MultiProcessConfig
}

// LineProcessConfig 多行处理器配置
type LineProcessConfig struct {
	Enable         *bool  `yaml:"enable,omitempty" default:"true"`
	Terminator     string `yaml:"terminator,omitempty" default:"auto"` // effect if Source is file or filestream
	Charset        string `yaml:"charset,omitempty" default:"utf-8"`
	MaxBackLogSize uint64 `yaml:"maxBackLogSize,omitempty" default:"131072"` //最大积压的字节数
}

// DecoderProcessConfig 解码器【】恶作剧哦哦
type DecoderProcessConfig struct {
	Charset string `yaml:"charset,omitempty" default:"utf-8"`
}

// MultiProcessConfig 行首匹配
type MultiProcessConfig struct {
	Enable         *bool  `yaml:"enable,omitempty" default:"false"`
	FirstPattern   string // effect if Source is file or filestream
	MaxBackLogSize uint64 `yaml:"maxBackLogSize,omitempty" default:"131072"` //最大挤压的字节数
}
