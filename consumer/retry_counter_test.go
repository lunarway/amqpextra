package consumer

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"testing"
)

func TestRetryCounter(main *testing.T) {

	main.Run("RetryCount is initially zero", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		rc := newRetryCounter(ctx)
		counter := rc.read()

		assert.Equal(t, 0, counter)
	})

	main.Run("RetryCount is incremented when receiving a unready message over its channel ", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		rc := newRetryCounter(ctx)
		assert.Equal(t, 0, rc.read())

		rc.ch <- State{
			Unready: &Unready{
				Err: fmt.Errorf("error"),
			},
		}

		assert.Equal(t, 1, rc.read())
	})

	main.Run("RetryCount is incremented and reset when receiving a unready and ready message over its channel ", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		rc := newRetryCounter(ctx)
		assert.Equal(t, 0, rc.read())

		rc.ch <- State{
			Unready: &Unready{
				Err: fmt.Errorf("error"),
			},
		}
		rc.ch <- State{
			Ready: &Ready{},
		}

		assert.Equal(t, 0, rc.read())
	})
}
