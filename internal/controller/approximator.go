package controller

import "github.com/pako-23/queue-scaler/internal/queue"

type ObserverState struct {
	State string
}

func NewObserverState() *ObserverState {
	return &ObserverState{
		State: queue.NewQueueNetwork().ToDOT(),
	}

}

func (o *ObserverState) Stabilize(state *queue.QueueNetwork) error {
	o.State = state.ToDOT()
	return nil
}
