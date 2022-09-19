package regex

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/source/codec"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_decoder(t *testing.T) {
	var err error
	codecConfig := &codec.Config{}
	codecConfig.Type = Type
	PruneBValue := false
	regexcodec := Config{
		Pattern:    "^(?P<time>[^ ^Z]+Z) (?P<stream>stdout|stderr) (?P<logtag>[^ ]*) (?P<log>.*)$",
		BodyFields: "log",
		Prune:      &PruneBValue,
	}
	r := &Regex{
		config: &regexcodec,
	}
	r.Init()

	var e api.Event = event.NewEvent(map[string]interface{}{}, []byte("2021-12-01T03:13:58.298476921Z stderr F INFO [main] Starting service [Catalina]"))
	eventData, err := r.Decode(e)
	assert.Equal(t, err, nil)
	assert.NotNil(t, eventData, nil)
	fmt.Println(eventData)
}
