package controller

import "github.com/pako-23/queue-scaler/internal/queue"

type Controller interface {
	Stabilize(*queue.QueueNetwork) error
}
