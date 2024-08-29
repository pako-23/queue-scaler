package controller

import "github.com/pako-23/queue-scaler/internal/queue"

type NullController struct{}

func (n *NullController) Stabilize(*queue.QueueNetwork) error { return nil }
