package consumer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRetryCounter(main *testing.T) {
	main.Run("RetryCount is initially zero", func(t *testing.T) {
		rc := newRetryCounter()
		counter := rc.read()

		assert.Equal(t, 0, counter)
	})

	main.Run("RetryCount is one when incremented once", func(t *testing.T) {
		rc := newRetryCounter()
		assert.Equal(t, 0, rc.read())

		rc.increment()

		assert.Equal(t, 1, rc.read())
	})

	main.Run("RetryCount is two when incremented twice", func(t *testing.T) {
		rc := newRetryCounter()
		assert.Equal(t, 0, rc.read())

		rc.increment()
		rc.increment()

		assert.Equal(t, 2, rc.read())
	})

	main.Run("RetryCount is zero when incremented and reset", func(t *testing.T) {
		rc := newRetryCounter()
		assert.Equal(t, 0, rc.read())

		rc.increment()
		rc.reset()

		assert.Equal(t, 0, rc.read())
	})
}
