package observer

import (
	"context"
	"log"
	"time"

	"github.com/pako-23/queue-scaler/internal/queue"
	"github.com/pako-23/queue-scaler/internal/receiver"
)

type trace map[string]*receiver.Span

func (t trace) completed() bool {
	for _, details := range t {
		if _, ok := t[details.Parent]; !ok && details.Parent != "" {
			return false
		}
	}

	return true
}

func processTraces(state *queue.QueueNetwork, traces map[string]trace) {
	for traceId, spans := range traces {
		if !spans.completed() {
			continue
		}

		for _, details := range spans {
			if details.Parent == "" {
				state.AddExternalRequest(details)
			} else {
				state.AddInternalRequest(spans[details.Parent], details)
			}
		}

		delete(traces, traceId)
	}

}

func (o *Observer) Observe(ctx context.Context, ch <-chan *receiver.Span) {
	traces := map[string]trace{}

	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			processTraces(o.State, traces)
			o.State.UpdateEstimates(o.Interval)

			if err := o.controller.Stabilize(o.State); err != nil {
				log.Println(err)
			}

		case span := <-ch:
			if span.ServiceName == "" {
				continue
			}

			if _, ok := traces[span.TraceId]; !ok {
				traces[span.TraceId] = trace{}
			}

			traces[span.TraceId][span.SpanId] = span

		case <-ctx.Done():
			return
		}
	}
}
