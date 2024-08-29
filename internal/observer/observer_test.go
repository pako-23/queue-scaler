package observer_test

import (
	"testing"
	"time"

	"github.com/pako-23/queue-scaler/internal/controller"
	"github.com/pako-23/queue-scaler/internal/observer"
	"gotest.tools/v3/assert"
)

func TestObserverInit(t *testing.T) {
	t.Parallel()

	t.Run("no option construct", func(t *testing.T) {
		obs := observer.NewObserver()
		assert.Assert(t, obs != nil)
		assert.Assert(t, obs.State != nil)
		assert.Assert(t, obs.Interval == observer.DefaultInterval)
	})

	t.Run("with controller", func(t *testing.T) {
		obs := observer.NewObserver(observer.WithController(&controller.NullController{}))
		assert.Assert(t, obs != nil)
		assert.Assert(t, obs.State != nil)
		assert.Assert(t, obs.Interval == observer.DefaultInterval)
	})

	t.Run("with interval", func(t *testing.T) {
		interval := observer.DefaultInterval + time.Second
		obs := observer.NewObserver(observer.WithInterval(interval))
		assert.Assert(t, obs != nil)
		assert.Assert(t, obs.State != nil)
		assert.Assert(t, obs.Interval == interval)
	})

	t.Run("with interval and controller", func(t *testing.T) {
		interval := observer.DefaultInterval + time.Second
		obs := observer.NewObserver(
			observer.WithController(&controller.NullController{}),
			observer.WithInterval(interval))
		assert.Assert(t, obs != nil)
		assert.Assert(t, obs.State != nil)
		assert.Assert(t, obs.Interval == interval)
	})
}
