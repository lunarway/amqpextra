package consumer

import (
	"context"
	"sync"
)

type retryCounter struct {
	mu         sync.RWMutex
	retryCount int

	ch         chan State
}

func newRetryCounter() *retryCounter {
	return &retryCounter{
		ch : make(chan State, 2),
	}
}

// updateLoop update the internal retry counter via notifications on its channel. It blocks until ctx is cancelled.
func (r *retryCounter) updateLoop(ctx context.Context)  {
	for {
		select {
		case state := <- r.ch:
			r.update(state)
		case <-ctx.Done():
			return
		}
	}
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