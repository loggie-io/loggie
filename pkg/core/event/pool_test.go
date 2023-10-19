package event

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
)

func TestGetAndPut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	const capacity = 10

	p := NewDefaultPool(capacity)

	// GetContext
	{
		var events []api.Event
		for i := 0; i < 10; i++ {
			event, ok := p.GetContext(ctx)
			if !ok {
				t.Fatal("GetContext failed")
			}
			events = append(events, event)
		}
		p.Put(events...)
	}
	// Get
	{
		var events []api.Event
		for i := 0; i < 10; i++ {
			event := p.Get()
			events = append(events, event)
		}
		p.Put(events...)
	}
	// GetNContext
	{
		events1, ok := p.GetNContext(ctx, capacity/2)
		if !ok {
			t.Fatal("GetNContext failed")
		}
		events2, ok := p.GetNContext(ctx, capacity/2)
		if !ok {
			t.Fatal("GetNContext failed")
		}
		p.Put(events1...)
		p.Put(events2...)
	}
	// GetN
	{
		events1 := p.GetN(capacity / 2)
		events2 := p.GetN(capacity / 2)
		p.Put(events1...)
		p.Put(events2...)
	}
}

func TestParallelGetAndPut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	p := NewDefaultPool(10)

	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			es, ok := p.GetNContext(ctx, 3)
			if !ok {
				t.Errorf("GetNContext failed")
			}
			time.Sleep(time.Millisecond)
			p.Put(es...)
			wg.Done()
		}()

	}
	wg.Wait()
}

func TestGetWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := NewDefaultPool(10)

	events := p.GetN(10)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_, ok := p.GetNContext(ctx, 9)
		if ok {
			t.Errorf("GetNContext abnormal")
		}
		wg.Done()
	}()
	p.Put(events[0])
	p.Put(events[1])
	p.Put(events[2])
	p.Put(events[3])
	p.Put(events[4])
	cancel()
	wg.Wait()
}

func benchmarkPoolIO(b *testing.B, poolCapacity, parallel, batchSize int) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	ch := make(chan []api.Event)
	p := NewDefaultPool(poolCapacity)

	wg.Add(parallel)
	for i := 0; i < parallel; i++ {
		go func() {
			defer wg.Done()
			for {
				events, ok := p.GetNContext(ctx, batchSize)
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
				case ch <- events:
				}
			}
		}()
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		events := <-ch
		p.Put(events...)
	}
	b.StopTimer()
	cancel()
	wg.Wait()
}

func BenchmarkPoolIO(b *testing.B) {
	const batchSize = 1024
	for i := 0; i < 10; i++ {
		parallel := 1 << (i + 1)
		b.Run(fmt.Sprintf("BenchmarkPoolIO%d", parallel), func(b *testing.B) {
			poolCapacity := batchSize * (parallel/2 + 1)
			benchmarkPoolIO(b, poolCapacity, parallel, batchSize)
		})
	}
}
