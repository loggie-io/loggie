package event

import (
	"container/list"
	"context"
	"errors"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/log"
)

type request struct {
	n      int
	result chan []api.Event
	ctx    context.Context
}

func newRequest(n int) *request {
	return &request{
		n:      n,
		result: make(chan []api.Event, 1),
	}
}

func newRequestWithCtx(n int, ctx context.Context) *request {
	return &request{
		n:      n,
		result: make(chan []api.Event, 1),
		ctx:    ctx,
	}
}

type fairEventPool struct {
	capacity    int
	free        int
	factory     Factory
	events      []api.Event
	requests    *list.List
	requestChan chan *request
	eventChan   chan api.Event
	done        chan struct{}
}

func NewFairEventPool(capacity int, factory Factory) *fairEventPool {
	pool := &fairEventPool{
		capacity:    capacity,
		free:        capacity,
		factory:     factory,
		events:      make([]api.Event, capacity),
		requests:    list.New(),
		requestChan: make(chan *request),
		done:        make(chan struct{}),
	}
	for i := 0; i < capacity; i++ {
		pool.events[i] = pool.factory()
	}
	go pool.run()
	return pool
}

func (f *fairEventPool) stop() {
	close(f.done)
}

func (f *fairEventPool) Put(event api.Event) {
	select {
	case <-f.done:
		return
	case f.eventChan <- event:
		return
	}
}

func (f *fairEventPool) PutAll(events []api.Event) {
	for _, e := range events {
		f.Put(e)
	}
}

func (f *fairEventPool) Get() api.Event {
	r := newRequest(1)
	var in = f.requestChan
	for {
		select {
		case <-f.done:
			return f.factory()
		case in <- r:
			in = nil
		case es := <-r.result:
			return es[0]
		}
	}
}

func (f *fairEventPool) GetC(ctx context.Context) (api.Event, error) {
	events, err := f.GetN(1, ctx)
	if err != nil {
		return nil, err
	}
	return events[0], err
}

func (f *fairEventPool) GetN(n int, ctx context.Context) ([]api.Event, error) {
	if n > f.capacity {
		log.Panic("n exceeds the capacity of the pool: %d > %d", n, f.capacity)
	}
	r := newRequestWithCtx(n, ctx)
	var in = f.requestChan
	for {
		select {
		case <-f.done:
			return nil, errors.New("event pool stop")
		case in <- r:
			in = nil
		case <-r.ctx.Done():
			return nil, errors.New("ctx cancel")
		case es := <-r.result:
			return es, nil
		}
	}
}

func (f *fairEventPool) run() {
	log.Info("event pool start")
	defer func() {
		log.Info("event pool stop")
	}()

	var last *request
	var resultChan chan []api.Event
	var es []api.Event
	var getForRequest = func(r *request) {
		n := r.n
		if f.free >= n {
			events := make([]api.Event, n)
			for i := 0; i < n; i++ {
				f.free--
				e := f.events[f.free]
				events[i] = e
			}
			es = events
			resultChan = r.result
		}
	}
	for {
		select {
		case <-f.done:
			return
		case r := <-f.requestChan:
			f.requests.PushBack(r)
		case e := <-f.eventChan:
			if f.free < f.capacity {
				f.events[f.free] = e
				f.free++
			}

			if last != nil && f.free >= last.n {
				getForRequest(last)
			}
		case resultChan <- es:
			last = nil
			resultChan = nil
			es = nil
		}
		if last == nil && f.requests.Len() > 0 {
			el := f.requests.Front()
			r := el.Value.(*request)
			n := r.n
			if f.free >= n {
				getForRequest(r)
			} else {
				last = r
			}
			f.requests.Remove(el)
		}
	}
}
