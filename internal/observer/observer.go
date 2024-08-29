package observer

import (
	"github.com/pako-23/queue-scaler/internal/controller"
	"github.com/pako-23/queue-scaler/internal/queue"
	"time"
)

const DefaultInterval = 5 * time.Second

type Observer struct {
	Interval   time.Duration
	State      *queue.QueueNetwork
	controller controller.Controller
}

type Option func(*Observer)

func NewObserver(options ...Option) *Observer {
	observer := &Observer{
		Interval:   DefaultInterval,
		State:      queue.NewQueueNetwork(),
		controller: &controller.NullController{},
	}

	for _, opt := range options {
		opt(observer)
	}

	return observer
}

func WithController(cont controller.Controller) Option {
	return func(observer *Observer) {
		observer.controller = cont
	}
}

func WithInterval(interval time.Duration) Option {
	return func(observer *Observer) {
		observer.Interval = interval
	}
}
