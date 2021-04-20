package consumer

import (
	"context"
	"sync"
)

type retryCounter struct {
	mu sync.RWMutex
	retryCount int

	ch chan State
}

func newRetryCounter(ctx context.Context) *retryCounter {
	r := &retryCounter{
		ch: make(chan State, 1),
	}

	go func() {
		for {
			select {
			case state := <- r.ch:
				r.update(state)
			case <-ctx.Done():
				return
			}
		}
	}()

	return r
}

func (r *retryCounter) read() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.retryCount
}

func (r *retryCounter) update(state State) {
	if state.Ready != nil {
		r.reset()
	}
	if state.Unready != nil {
		r.increment()
	}
}

func (r *retryCounter) increment() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.retryCount++
}

func (r *retryCounter) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.retryCount = 0
}