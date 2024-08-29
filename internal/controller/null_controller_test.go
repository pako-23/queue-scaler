package controller_test

import (
	"testing"

	"github.com/pako-23/queue-scaler/internal/controller"
	"github.com/pako-23/queue-scaler/internal/queue"
	"gotest.tools/v3/assert"
)

func TestNullController(t *testing.T) {
	var cont controller.Controller = &controller.NullController{}

	t.Parallel()

	t.Run("no state", func(t *testing.T) {
		assert.Assert(t, cont.Stabilize(nil) == nil)
	})

	t.Run("empty state", func(t *testing.T) {
		assert.Assert(t, cont.Stabilize(queue.NewQueueNetwork()) == nil)
	})

}
