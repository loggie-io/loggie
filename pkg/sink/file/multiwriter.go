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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/panjf2000/ants/v2"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	LineEnding = []byte("\n")
)

type Options struct {
	WorkerCount int
	MaxSize     int
	MaxAge      int
	MaxBackups  int
	LocalTime   bool
	Compress    bool
	IdleTimeout time.Duration
}

type Message struct {
	Filename string
	Data     []byte
}

type MultiFileWriter struct {
	opt *Options

	// Manages the current set of writers.
	wsMu    sync.Mutex
	writers map[string]*fw
	workers *ants.Pool

	closed uint32 // Atomic flag indicating whether the writer has been closed.

	ticker *time.Ticker
	close  chan struct{} // closed when flushLoop should stop
	done   chan struct{} // closed when flushLoop has stopped
}

func NewMultiFileWriter(opt *Options) (*MultiFileWriter, error) {
	pool, err := ants.NewPool(
		opt.WorkerCount,
		ants.WithExpiryDuration(60*time.Second),
		ants.WithPreAlloc(true),
	)
	if err != nil {
		return nil, err
	}
	w := &MultiFileWriter{
		opt:     opt,
		writers: make(map[string]*fw, opt.WorkerCount),
		workers: pool,
		ticker:  time.NewTicker(30 * time.Second),
		close:   make(chan struct{}),
		done:    make(chan struct{}),
	}
	go w.flushLoop()
	return w, nil
}

func (w *MultiFileWriter) Write(msgs ...Message) error {
	if len(msgs) == 0 {
		return nil
	}

	if w.isClosed() {
		return nil
	}

	assignments := make(map[string][]int32)
	for i := range msgs {
		assignments[msgs[i].Filename] = append(assignments[msgs[i].Filename], int32(i))
	}

	for key, indexes := range assignments {
		writer := w.getWriter(key)
		is := indexes
		w.workers.Submit(func() {
			for _, i := range is {
				if _, err := writer.Write(msgs[i].Data); err != nil {
					log.Error("write error: %+v", err)
				}
				if _, err := writer.Write(LineEnding); err != nil {
					log.Error("write error: %+v", err)
				}
			}
		})
	}
	return nil
}

func (w *MultiFileWriter) Close() error {
	err := w.markClosed()
	if err != nil {
		return err
	}
	w.ticker.Stop()
	close(w.close) // tell flushLoop to stop
	<-w.done       // and wait until it has
	w.wsMu.Lock()
	for _, v := range w.writers {
		if v == nil {
			continue
		}
		v.Close()
	}
	w.wsMu.Unlock()
	w.workers.Release()
	return nil
}

// flushLoop flushes the buffer at the configured interval until Stop is
// called.
func (w *MultiFileWriter) flushLoop() {
	defer close(w.done)

	for {
		select {
		case <-w.ticker.C:
			var tmp []*fw
			w.wsMu.Lock()
			tmp = make([]*fw, 0, len(w.writers))
			for _, v := range w.writers {
				if v != nil {
					tmp = append(tmp, v)
				}
			}
			w.wsMu.Unlock()
			for i := range tmp {
				tmp[i].Sync()
			}
		case <-w.close:
			return
		}
	}
}

func (w *MultiFileWriter) getWriter(fn string) *Writer {
	w.wsMu.Lock()
	defer w.wsMu.Unlock()
	v, ok := w.writers[fn]
	if !ok {
		wc := &lumberjack.Logger{
			Filename:   fn,
			MaxSize:    w.opt.MaxSize, // megabytes
			MaxBackups: w.opt.MaxBackups,
			MaxAge:     w.opt.MaxAge, // days
			LocalTime:  w.opt.LocalTime,
			Compress:   w.opt.Compress, // disabled by default
		}
		v = &fw{
			Writer: &Writer{
				W:                 wc,
				AutoFlushDisabled: true,
			},
			filename: fn,
			wc:       wc,
		}
		v.timer = time.AfterFunc(w.opt.IdleTimeout, func() {
			w.wsMu.Lock()
			defer w.wsMu.Unlock()
			delete(w.writers, v.filename)
			v.Close()
		})
		w.writers[fn] = v
	} else {
		v.timer.Reset(w.opt.IdleTimeout)
	}
	return v.Writer
}

func (w *MultiFileWriter) markClosed() error {
	if !atomic.CompareAndSwapUint32(&w.closed, 0, 1) {
		return io.ErrClosedPipe
	}
	return nil
}

func (w *MultiFileWriter) isClosed() bool {
	return atomic.LoadUint32(&w.closed) == 1
}

type fw struct {
	*Writer

	filename string
	wc       *lumberjack.Logger
	timer    *time.Timer
}

func (f *fw) Close() error {
	if f.timer != nil {
		f.timer.Stop()
	}
	if err := f.Stop(); err != nil {
		log.Error("stop file(name:%s) writer error, err: %v", f.filename, err)
	}

	if err := f.wc.Close(); err != nil {
		log.Error("close file(name:%s) writer error, err: %v", f.filename, err)
	}
	return nil
}
