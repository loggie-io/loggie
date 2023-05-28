package gjson

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/codec"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_decoder(t *testing.T) {
	log.InitDefaultLogger()
	var err error
	codecConfig := &codec.Config{}
	codecConfig.Type = Type
	PruneBValue := false
	regexcodec := Config{
		BodyFields: "log",
		Prune:      &PruneBValue,
	}
	j := &GJson{
		config: &regexcodec,
	}
	j.Init()

	var e api.Event = event.NewEvent(map[string]interface{}{}, []byte("{\"log\":\"level=warn ts=2022-08-01T07:44:32.895Z caller=main.go:322 msg=\\\"unable to leave gossip mesh\\\" err=\\\"timeout waiting for leave broadcast\\\"\\n\",\"stream\":\"stderr\",\"time\":\"2022-08-01T07:44:32.896400037Z\"}"))
	eventData, err := j.Decode(e)
	assert.Equal(t, err, nil)
	assert.NotNil(t, eventData, nil)
	assert.Contains(t, eventData.Header(), "time")
	assert.Contains(t, eventData.Header(), "stream")
}
