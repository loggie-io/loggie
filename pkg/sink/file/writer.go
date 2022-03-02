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

package file

import (
	"bufio"
	"io"
	"sync"
	"time"
)

const (
	// _defaultBufferSize specifies the default size used by Buffer.
	_defaultBufferSize = 256 * 1024 // 256 kB

	// _defaultFlushInterval specifies the default flush interval for
	// Buffer.
	_defaultFlushInterval = 30 * time.Second
)

type Writer struct {
	// This field is required.
	W io.Writer

	// Size specifies the maximum amount of data the writer will buffered
	// before flushing.
	//
	// Defaults to 256 kB if unspecified.
	Size int

	// AutoFlushDisabled whether to disable automatic flush
	AutoFlushDisabled bool
	// FlushInterval specifies how often the writer should flush data if
	// there have been no writes.
	//
	// Defaults to 30 seconds if unspecified.
	FlushInterval time.Duration

	// unexported fields for state
	mu          sync.Mutex
	initialized bool // whether initialize() has run
	stopped     bool // whether Stop() has run
	writer      *bufio.Writer
	ticker      *time.Ticker
	stop        chan struct{} // closed when flushLoop should stop
	done        chan struct{} // closed when flushLoop has stopped
}

func (w *Writer) initialize() {
	size := w.Size
	if size == 0 {
		size = _defaultBufferSize
	}

	w.writer = bufio.NewWriterSize(w.W, size)
	w.initialized = true

	if !w.AutoFlushDisabled {
		flushInterval := w.FlushInterval
		if flushInterval == 0 {
			flushInterval = _defaultFlushInterval
		}
		w.ticker = time.NewTicker(flushInterval)
		w.stop = make(chan struct{})
		w.done = make(chan struct{})
		go w.flushLoop()
	}
}

// Write writes log data into buffer syncer directly, multiple Write calls will be batched,
// and log data will be flushed to disk when the buffer is full or periodically.
func (w *Writer) Write(bs []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stopped {
		return 0, nil
	}

	if !w.initialized {
		w.initialize()
	}

	// To avoid partial writes from being flushed, we manually flush the existing buffer if:
	// * The current write doesn't fit into the buffer fully, and
	// * The buffer is not empty (since bufio will not split large writes when the buffer is empty)
	if len(bs) > w.writer.Available() && w.writer.Buffered() > 0 {
		if err := w.writer.Flush(); err != nil {
			return 0, err
		}
	}

	return w.writer.Write(bs)
}

// Sync flushes buffered log data into disk directly.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.initialized {
		return w.writer.Flush()
	}

	return nil
}

// flushLoop flushes the buffer at the configured interval until Stop is
// called.
func (w *Writer) flushLoop() {
	defer close(w.done)

	for {
		select {
		case <-w.ticker.C:
			// we just simply ignore error here
			// because the underlying bufio writer stores any errors
			// and we return any error from Sync() as part of the close
			_ = w.Sync()
		case <-w.stop:
			return
		}
	}
}

// Stop closes the buffer, cleans up background goroutines, and flushes
// remaining unwritten data.
func (w *Writer) Stop() (err error) {
	var stopped bool

	// Critical section.
	func() {
		w.mu.Lock()
		defer w.mu.Unlock()

		if !w.initialized {
			return
		}

		stopped = w.stopped
		if stopped {
			return
		}
		w.stopped = true

		if !w.AutoFlushDisabled {
			w.ticker.Stop()
			close(w.stop) // tell flushLoop to stop
			<-w.done      // and wait until it has
		}
	}()

	// Don't call Sync on consecutive Stops.
	if !stopped {
		err = w.Sync()
	}

	return err
}
