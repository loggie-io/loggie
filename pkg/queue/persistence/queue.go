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

// Package persistence implements a disk-backed (persistent) queue.
//
// Every event is appended to an embedded badger store (write-ahead style)
// before it is handed over to the sink, and removed only after the sink
// consumed the batch successfully. On restart, events that were written but
// never acknowledged are replayed, so a sink outage or a crash of Loggie
// does not lose events that already entered the queue (at-least-once).
package persistence

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/batch"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/sink"
	"github.com/loggie-io/loggie/pkg/core/spi"
	"github.com/loggie-io/loggie/pkg/eventbus"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

const (
	Type = "persistence"

	// ReplaySourceName marks events replayed from disk after a restart.
	// They do not belong to any running source, so the pipeline must not
	// commit them to a source after the sink consumed them.
	ReplaySourceName = "@persistenceQueueReplay"
	// OriginSourceKey keeps the original source name of a replayed event in its meta.
	OriginSourceKey = "persistenceOriginSource"

	batchMetaFirstSeq = "persistenceQueueFirstSeq"
	batchMetaLastSeq  = "persistenceQueueLastSeq"

	ackInterceptorName = "persistenceQueueAck"

	defaultQueueName = "default"
)

func init() {
	pipeline.Register(api.QUEUE, Type, makeQueue)
}

func makeQueue(info pipeline.Info) api.Component {
	return &Queue{
		config:       &Config{},
		pipelineName: info.PipelineName,
		sinkCount:    info.SinkCount,
		listeners:    info.R.LoadQueueListeners(),
	}
}

type walItem struct {
	seq   uint64
	event api.Event
}

type seqRange struct {
	first uint64
	last  uint64
}

type Queue struct {
	pipelineName string
	sinkCount    int
	config       *Config
	name         string
	done         chan struct{}
	in           chan api.Event
	wal          chan walItem
	ackC         chan seqRange
	out          chan api.Batch
	listeners    []spi.QueueListener
	countDown    *sync.WaitGroup

	db       *badger.DB
	writeSeq uint64 // next sequence to assign, only accessed by the writer goroutine

	diskBytes int64 // atomic: bytes of un-acked events on disk
	diskCount int64 // atomic: count of un-acked events on disk

	// snapshot of the leftover data found at startup, replayed by the replay goroutine
	replayFrom  uint64
	replayTo    uint64
	replayCount int64

	spaceMu   sync.Mutex
	spaceCond *sync.Cond
	stopped   bool
}

func (q *Queue) Type() api.Type {
	return Type
}

func (q *Queue) Category() api.Category {
	return api.QUEUE
}

func (q *Queue) Config() interface{} {
	return q.config
}

func (q *Queue) String() string {
	return fmt.Sprintf("%s/%s", api.QUEUE, Type)
}

func (q *Queue) Init(context api.Context) error {
	q.done = make(chan struct{})
	q.name = context.Name()
	if q.name == "" {
		q.name = defaultQueueName
	}
	q.countDown = &sync.WaitGroup{}
	q.spaceCond = sync.NewCond(&q.spaceMu)

	dir := filepath.Join(q.config.DataDir, q.pipelineName, q.name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.WithMessagef(err, "create persistence queue dir %s", dir)
	}

	opts := badger.DefaultOptions(dir).
		WithSyncWrites(q.config.SyncWrites).
		WithLogger(&badgerLogger{}).
		WithCompactL0OnClose(true).
		WithNumVersionsToKeep(1)
	opts.MemTableSize = 16 << 20
	opts.ValueLogFileSize = 100 << 20

	db, err := badger.Open(opts)
	if err != nil {
		return errors.WithMessagef(err, "open persistence queue db %s", dir)
	}
	q.db = db

	if err := q.scanExisting(); err != nil {
		return errors.WithMessage(err, "scan persistence queue db")
	}

	log.Info("%s(%s) using dir %s; sink count: %d; batch size: %d; max bytes: %d; leftover events: %d",
		q.String(), q.pipelineName, dir, q.sinkCount, q.config.BatchSize, q.config.MaxBytes, q.replayCount)

	q.out = make(chan api.Batch, q.sinkCount)
	q.in = make(chan api.Event, 16)
	q.wal = make(chan walItem, q.config.WriteBufferSize)
	q.ackC = make(chan seqRange, q.sinkCount*2)
	return nil
}

// scanExisting restores the sequence counter and backlog gauges from the data
// left on disk by the previous run, and remembers the range to be replayed.
func (q *Queue) scanExisting() error {
	var (
		minSeq, maxSeq uint64
		count, bytes   int64
	)
	err := q.db.View(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = false
		it := txn.NewIterator(itOpts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			seq, ok := decodeKey(item.Key())
			if !ok {
				continue
			}
			if count == 0 {
				minSeq = seq
			}
			maxSeq = seq
			count++
			bytes += item.ValueSize()
		}
		return nil
	})
	if err != nil {
		return err
	}

	q.writeSeq = maxSeq + 1
	q.replayFrom = minSeq
	q.replayTo = maxSeq
	q.replayCount = count
	atomic.StoreInt64(&q.diskCount, count)
	atomic.StoreInt64(&q.diskBytes, bytes)
	return nil
}

func (q *Queue) Start() error {
	var listeners strings.Builder
	for _, listener := range q.listeners {
		listeners.WriteString(listener.Name())
		listeners.WriteString(" ")
	}
	log.Info("queue listeners: %s", listeners.String())

	go q.writer()
	go q.aggregator()
	go q.ackWorker()
	go q.valueLogGC()
	if q.replayCount > 0 {
		go q.replay()
	}
	return nil
}

func (q *Queue) Stop() {
	q.spaceMu.Lock()
	q.stopped = true
	q.spaceCond.Broadcast()
	q.spaceMu.Unlock()

	close(q.done)
	q.countDown.Wait()
	if q.db != nil {
		if err := q.db.Close(); err != nil {
			log.Error("close persistence queue db: %v", err)
		}
	}
	log.Info("[%s]persistence queue stop", q.pipelineName)
}

func (q *Queue) In(event api.Event) {
	q.in <- event
}

func (q *Queue) Out() api.Batch {
	return <-q.out
}

func (q *Queue) OutChan() chan api.Batch {
	return q.out
}

// DependencyInterceptors injects an ack interceptor into the sink invoker chain:
// once the sink consumed (or dropped) a batch, the events of the batch are
// removed from disk.
func (q *Queue) DependencyInterceptors() []api.Interceptor {
	ack := &sink.AbstractInterceptor{
		DoName: func() string {
			return ackInterceptorName
		},
		DoIntercept: func(invoker sink.Invoker, invocation sink.Invocation) api.Result {
			r := invoker.Invoke(invocation)
			status := r.Status()
			if status == api.SUCCESS || status == api.DROP {
				q.commitBatch(invocation.Batch)
			}
			return r
		},
	}
	return []api.Interceptor{ack}
}

func (q *Queue) commitBatch(b api.Batch) {
	meta := b.Meta()
	if meta == nil {
		return
	}
	first, fok := meta[batchMetaFirstSeq].(uint64)
	last, lok := meta[batchMetaLastSeq].(uint64)
	if !fok || !lok {
		return
	}
	select {
	case q.ackC <- seqRange{first: first, last: last}:
	case <-q.done:
	}
}

// writer appends incoming events to disk in aggregated write batches,
// then forwards them to the in-memory aggregator.
func (q *Queue) writer() {
	q.countDown.Add(1)
	defer func() {
		q.countDown.Done()
		log.Info("persistence queue(%s) writer stop", q.String())
	}()

	bufSize := q.config.WriteBufferSize
	buffer := make([]api.Event, 0, bufSize)
	for {
		select {
		case <-q.done:
			// persist events already handed to the queue before shutting down
			for {
				select {
				case e := <-q.in:
					buffer = append(buffer, e)
				default:
					if len(buffer) > 0 {
						q.persist(buffer, false)
					}
					return
				}
			}
		case e := <-q.in:
			buffer = append(buffer, e)
		drain:
			for len(buffer) < bufSize {
				select {
				case next := <-q.in:
					buffer = append(buffer, next)
				default:
					break drain
				}
			}
			q.waitForSpace()
			q.persist(buffer, true)
			buffer = buffer[:0]
		}
	}
}

// waitForSpace blocks while the on-disk backlog exceeds MaxBytes,
// propagating backpressure to the sources through In().
func (q *Queue) waitForSpace() {
	q.spaceMu.Lock()
	warned := false
	for !q.stopped && atomic.LoadInt64(&q.diskBytes) >= q.config.MaxBytes {
		if !warned {
			log.Warn("persistence queue(%s) backlog exceeds maxBytes(%d), blocking sources",
				q.pipelineName, q.config.MaxBytes)
			warned = true
		}
		q.spaceCond.Wait()
	}
	q.spaceMu.Unlock()
}

func (q *Queue) persist(events []api.Event, forward bool) {
	wb := q.db.NewWriteBatch()
	defer wb.Cancel()

	items := make([]walItem, 0, len(events))
	var bytes int64
	var count int64
	for _, e := range events {
		seq := q.writeSeq
		q.writeSeq++
		val, err := marshalEvent(e)
		if err != nil {
			// the event cannot be persisted; deliver it downstream anyway
			log.Warn("marshal event failed, event will not be persisted: %v", err)
		} else if err = wb.Set(encodeKey(seq), val); err != nil {
			log.Error("persist event failed: %v", err)
		} else {
			bytes += int64(len(val))
			count++
		}
		items = append(items, walItem{seq: seq, event: e})
	}
	if err := wb.Flush(); err != nil {
		log.Error("flush persistence queue write batch failed: %v", err)
	}
	atomic.AddInt64(&q.diskBytes, bytes)
	atomic.AddInt64(&q.diskCount, count)

	if !forward {
		return
	}
	for _, item := range items {
		select {
		case q.wal <- item:
		case <-q.done:
			return
		}
	}
}

// aggregator collects persisted events into batches and pushes them to the sinks,
// stamping each batch with the sequence range it covers.
func (q *Queue) aggregator() {
	q.countDown.Add(1)
	log.Info("persistence queue aggregator start")
	timeout := q.config.BatchAggMaxTimeout
	flusher := time.NewTicker(timeout)
	defer func() {
		flusher.Stop()
		q.countDown.Done()
		log.Info("persistence queue(%s) aggregator stop", q.String())
	}()

	batchBytes := q.config.BatchBytes
	batchSize := q.config.BatchSize
	buffer := make([]api.Event, 0, batchSize)
	size := 0
	bytes := int64(0)
	var firstSeq, lastSeq uint64
	firstEventAppendTime := time.Now()
	flush := func() {
		q.beforeQueueConvertBatch(buffer)
		b := batch.NewBatchWithEvents(buffer)
		b.Meta()[batchMetaFirstSeq] = firstSeq
		b.Meta()[batchMetaLastSeq] = lastSeq
		q.out <- b
		buffer = make([]api.Event, 0, batchSize)
		size = 0
		bytes = 0
	}
	for {
		select {
		case <-q.done:
			q.publishMetric()
			return
		case item := <-q.wal:
			if size == 0 {
				firstEventAppendTime = time.Now()
				firstSeq = item.seq
			}
			lastSeq = item.seq
			buffer = append(buffer, item.event)
			size++
			bytes += int64(len(item.event.Body()))
			if size >= batchSize || bytes >= batchBytes {
				flush()
			}
		case <-flusher.C:
			// Instead of going to flush directly, check whether the first event of batch append time exceeds the timeout.
			// In order to ensure the integrity of batch as much as possible
			// if size>0, firstEventAppendTime must be updated
			if size > 0 && time.Since(firstEventAppendTime) > timeout {
				flush()
			}
			q.publishMetric()
		}
	}
}

func (q *Queue) publishMetric() {
	eventbus.PublishOrDrop(eventbus.QueueMetricTopic, eventbus.QueueMetricData{
		PipelineName: q.pipelineName,
		Type:         string(q.Type()),
		Capacity:     q.config.MaxBytes,
		Size:         atomic.LoadInt64(&q.diskBytes),
	})
}

// ackWorker removes acknowledged sequence ranges from disk.
func (q *Queue) ackWorker() {
	q.countDown.Add(1)
	defer func() {
		q.countDown.Done()
		log.Info("persistence queue(%s) ack worker stop", q.String())
	}()

	for {
		select {
		case <-q.done:
			// process acks already emitted by the sinks before shutting down
			for {
				select {
				case r := <-q.ackC:
					q.deleteRange(r)
				default:
					return
				}
			}
		case r := <-q.ackC:
			q.deleteRange(r)
		}
	}
}

func (q *Queue) deleteRange(r seqRange) {
	var (
		keys  [][]byte
		bytes int64
	)
	err := q.db.View(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = false
		it := txn.NewIterator(itOpts)
		defer it.Close()
		for it.Seek(encodeKey(r.first)); it.Valid(); it.Next() {
			item := it.Item()
			seq, ok := decodeKey(item.Key())
			if !ok || seq > r.last {
				break
			}
			keys = append(keys, item.KeyCopy(nil))
			bytes += item.ValueSize()
		}
		return nil
	})
	if err != nil {
		log.Error("scan acked range failed: %v", err)
		return
	}
	if len(keys) == 0 {
		return
	}

	wb := q.db.NewWriteBatch()
	defer wb.Cancel()
	for _, key := range keys {
		if err = wb.Delete(key); err != nil {
			log.Error("delete acked event failed: %v", err)
		}
	}
	if err = wb.Flush(); err != nil {
		log.Error("flush acked deletion failed: %v", err)
		return
	}

	atomic.AddInt64(&q.diskBytes, -bytes)
	atomic.AddInt64(&q.diskCount, -int64(len(keys)))

	q.spaceMu.Lock()
	q.spaceCond.Broadcast()
	q.spaceMu.Unlock()
}

// replay pushes the events left on disk by the previous run to the sinks.
func (q *Queue) replay() {
	q.countDown.Add(1)
	defer func() {
		q.countDown.Done()
		log.Info("persistence queue(%s) replay stop", q.String())
	}()
	log.Info("persistence queue(%s) replaying %d leftover events, seq range [%d, %d]",
		q.pipelineName, q.replayCount, q.replayFrom, q.replayTo)

	cursor := q.replayFrom
	for cursor <= q.replayTo {
		select {
		case <-q.done:
			return
		default:
		}

		events, r, next, err := q.readChunk(cursor)
		if err != nil {
			log.Error("read replay chunk failed: %v", err)
			return
		}
		if next == cursor {
			// nothing left in range
			return
		}
		cursor = next

		if len(events) == 0 {
			// the whole chunk was corrupted: drop it from disk
			q.commitRange(r)
			continue
		}
		b := batch.NewBatchWithEvents(events)
		b.Meta()[batchMetaFirstSeq] = r.first
		b.Meta()[batchMetaLastSeq] = r.last
		select {
		case q.out <- b:
		case <-q.done:
			return
		}
	}
	log.Info("persistence queue(%s) replay done", q.pipelineName)
}

// readChunk reads up to a batch worth of events starting at cursor, bounded by
// replayTo. It returns the decoded events, the sequence range consumed and the
// next cursor. Short-lived read transactions keep badger GC unblocked even
// when the sinks consume slowly.
func (q *Queue) readChunk(cursor uint64) (events []api.Event, r seqRange, next uint64, err error) {
	batchSize := q.config.BatchSize
	batchBytes := q.config.BatchBytes
	next = cursor
	var bytes int64
	err = q.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(encodeKey(cursor)); it.Valid(); it.Next() {
			item := it.Item()
			seq, ok := decodeKey(item.Key())
			if !ok {
				continue
			}
			if seq > q.replayTo {
				break
			}
			if r.first == 0 {
				r.first = seq
			}
			r.last = seq
			next = seq + 1

			val, verr := item.ValueCopy(nil)
			if verr != nil {
				log.Warn("read replay event %d failed: %v", seq, verr)
				continue
			}
			e, derr := unmarshalEvent(val, q.pipelineName)
			if derr != nil {
				log.Warn("decode replay event %d failed, event dropped: %v", seq, derr)
				continue
			}
			events = append(events, e)
			bytes += int64(len(e.Body()))
			if len(events) >= batchSize || bytes >= batchBytes {
				break
			}
		}
		return nil
	})
	if next == cursor && next <= q.replayTo {
		// no keys found from cursor on: skip the rest of the range
		next = q.replayTo + 1
	}
	return events, r, next, err
}

func (q *Queue) commitRange(r seqRange) {
	if r.first == 0 && r.last == 0 {
		return
	}
	select {
	case q.ackC <- r:
	case <-q.done:
	}
}

func (q *Queue) valueLogGC() {
	q.countDown.Add(1)
	defer q.countDown.Done()

	interval := q.config.ValueLogGCInterval
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-q.done:
			return
		case <-ticker.C:
			if err := q.db.RunValueLogGC(0.5); err != nil {
				log.Debug("persistence queue value log gc: %v", err)
			}
		}
	}
}

func (q *Queue) beforeQueueConvertBatch(events []api.Event) {
	for _, listener := range q.listeners {
		listener.BeforeQueueConvertBatch(events)
	}
}

type badgerLogger struct {
}

func (l *badgerLogger) Errorf(format string, args ...interface{}) {
	log.Error(format, args...)
}

func (l *badgerLogger) Warningf(format string, args ...interface{}) {
	log.Warn(format, args...)
}

func (l *badgerLogger) Infof(format string, args ...interface{}) {
	log.Debug(format, args...)
}

func (l *badgerLogger) Debugf(format string, args ...interface{}) {
	log.Debug(format, args...)
}
