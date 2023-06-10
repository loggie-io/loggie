package json

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/codec"
)

var (
	json = jsoniter.ConfigFastest
)

func makeJsonCodec() codec.Codec {
	return NewJson()
}

func NewJson() *Json {
	return &Json{
		config: &Config{},
	}
}

func (j *Json) Config() interface{} {
	return j.config
}

func (j *Json) Init() {
}

func (j *Json) Decode(e api.Event) (api.Event, error) {
	if len(e.Body()) == 0 {
		return e, nil
	}
	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	if err := json.Unmarshal(e.Body(), &header); err != nil {
		log.Error("source codec json unmarshal error: %v", err)
		log.Debug("body: %s", string(e.Body()))
		return nil, err
	}

	body, err := getBytes(header, j.config.BodyFields)
	if len(body) == 0 {
		return e, nil
	}
	if err != nil {
		return e, err
	}
	body = pruneCLRF(body)

	// prune mode
	if j.config.Prune == nil || *j.config.Prune == true {
		e.Fill(e.Meta(), nil, body)
		return e, nil
	}

	delete(header, j.config.BodyFields)
	e.Fill(e.Meta(), e.Header(), body)
	return e, nil
}
