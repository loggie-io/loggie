/*
Copyright 2026 Loggie Authors

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

package persistence

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/context"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

func TestMain(m *testing.M) {
	log.InitDefaultLogger()
	m.Run()
}

func testConfig(dataDir string) *Config {
	return &Config{
		BatchSize:          4,
		BatchBytes:         32 * 1024 * 1024,
		BatchAggMaxTimeout: 100 * time.Millisecond,
		DataDir:            dataDir,
		MaxBytes:           1 << 30,
		WriteBufferSize:    64,
		ValueLogGCInterval: time.Hour,
	}
}

func newTestQueue(t *testing.T, dataDir string) *Queue {
	t.Helper()
	q, ok := makeQueue(pipeline.Info{
		PipelineName: "test-pipeline",
		SinkCount:    1,
		R:            pipeline.NewRegisterCenter(),
	}).(*Queue)
	assert.True(t, ok)
	q.config = testConfig(dataDir)

	err := q.Init(context.NewContext("test-queue", api.Type(Type), api.QUEUE, nil))
	assert.NoError(t, err)
	assert.NoError(t, q.Start())
	return q
}

func newTestEvent(source string, body string) api.Event {
	meta := event.NewDefaultMeta()
	meta.Set(event.SystemPipelineKey, "test-pipeline")
	meta.Set(event.SystemSourceKey, source)
	e := event.NewEvent(map[string]interface{}{"topic": "loggie"}, []byte(body))
	e.Fill(meta, e.Header(), e.Body())
	return e
}

func outWithTimeout(t *testing.T, q *Queue, timeout time.Duration) api.Batch {
	t.Helper()
	select {
	case b := <-q.OutChan():
		return b
	case <-time.After(timeout):
		t.Fatal("timed out waiting for batch")
		return nil
	}
}

func TestSerdeRoundTrip(t *testing.T) {
	e := newTestEvent("src-a", "hello loggie")
	data, err := marshalEvent(e)
	assert.NoError(t, err)

	restored, err := unmarshalEvent(data, "test-pipeline")
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello loggie"), restored.Body())
	assert.Equal(t, "loggie", restored.Header()["topic"])
	assert.Equal(t, ReplaySourceName, restored.Meta().Source())

	origin, exist := restored.Meta().Get(OriginSourceKey)
	assert.True(t, exist)
	assert.Equal(t, "src-a", origin)
}

func TestInOutAck(t *testing.T) {
	dataDir := t.TempDir()
	q := newTestQueue(t, dataDir)

	for i := 0; i < 4; i++ {
		q.In(newTestEvent("src-a", fmt.Sprintf("event-%d", i)))
	}

	b := outWithTimeout(t, q, 5*time.Second)
	assert.Len(t, b.Events(), 4)
	assert.Equal(t, int64(4), atomic.LoadInt64(&q.diskCount))

	// simulate a successful sink consumption
	q.commitBatch(b)
	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&q.diskCount) == 0
	}, 5*time.Second, 10*time.Millisecond, "acked events should be removed from disk")
	assert.Equal(t, int64(0), atomic.LoadInt64(&q.diskBytes))

	q.Stop()

	// nothing was left un-acked: a fresh queue must not replay anything
	q2 := newTestQueue(t, dataDir)
	assert.Equal(t, int64(0), q2.replayCount)
	q2.Stop()
}

func TestReplayAfterRestart(t *testing.T) {
	dataDir := t.TempDir()
	q := newTestQueue(t, dataDir)

	for i := 0; i < 3; i++ {
		q.In(newTestEvent("src-a", fmt.Sprintf("event-%d", i)))
	}

	// take the batch but never ack it: simulates a crash before the sink succeeded
	b := outWithTimeout(t, q, 5*time.Second)
	assert.Len(t, b.Events(), 3)
	q.Stop()

	q2 := newTestQueue(t, dataDir)
	assert.Equal(t, int64(3), q2.replayCount)

	rb := outWithTimeout(t, q2, 5*time.Second)
	assert.Len(t, rb.Events(), 3)
	bodies := make(map[string]bool)
	for _, e := range rb.Events() {
		bodies[string(e.Body())] = true
		assert.Equal(t, ReplaySourceName, e.Meta().Source())
	}
	for i := 0; i < 3; i++ {
		assert.True(t, bodies[fmt.Sprintf("event-%d", i)])
	}

	// ack the replayed batch, restart again: backlog must be empty
	q2.commitBatch(rb)
	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&q2.diskCount) == 0
	}, 5*time.Second, 10*time.Millisecond)
	q2.Stop()

	q3 := newTestQueue(t, dataDir)
	assert.Equal(t, int64(0), q3.replayCount)
	q3.Stop()
}
