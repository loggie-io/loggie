package regex

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/codec"
	"github.com/loggie-io/loggie/pkg/util"
	"regexp"
)

const (
	Type = "regex"
)

type Regex struct {
	config *Config

	regex *regexp.Regexp
}

type Config struct {
	Pattern    string `yaml:"pattern,omitempty" validate:"required"`
	BodyFields string `yaml:"bodyFields,omitempty" validate:"required"` // use the fields as `Body`
	Prune      *bool  `yaml:"prune,omitempty"`                          // we drop all the fields except `Body` in default
}

func (c *Config) Validate() error {
	if _, err := util.CompilePatternWithJavaStyle(c.Pattern); err != nil {
		return err
	}
	return nil
}

func init() {
	codec.Register(Type, makeRegexCodec)
}

func makeRegexCodec() codec.Codec {
	return NewRegex()
}

func NewRegex() *Regex {
	return &Regex{
		config: &Config{},
	}
}

func (j *Regex) Config() interface{} {
	return j.config
}

func (j *Regex) Init() {
	j.regex = util.MustCompilePatternWithJavaStyle(j.config.Pattern)
}

func (j *Regex) Decode(e api.Event) (api.Event, error) {
	paramsMap := util.MatchGroupWithRegex(j.regex, string(e.Body()))

	if len(paramsMap) == 0 {
		log.Error("match group with regex %s is empty", j.config.Pattern)
		log.Debug("body: %s", e.Body())
		return e, nil
	}

	if j.config.Prune == nil || *j.config.Prune == true {
		body, ok := paramsMap[j.config.BodyFields]
		if !ok {
			log.Debug("cannot find bodyFields %s", j.config.BodyFields)
			log.Debug("body: %s", e.Body())
			return e, nil
		}

		e.Fill(e.Meta(), e.Header(), []byte(body))
		return e, nil
	}

	// TODO add all the params to header when refactor multiline

	return e, nil
}
