package observer_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pako-23/queue-scaler/internal/observer"
	"github.com/pako-23/queue-scaler/internal/queue"
	"github.com/pako-23/queue-scaler/internal/receiver"
	"gotest.tools/v3/assert"
)

var errController = errors.New("")

type testController struct {
	queue *queue.QueueNetwork
	fail  bool
}

func (t *testController) Stabilize(state *queue.QueueNetwork) error {
	if t.fail {
		return errController
	}

	t.queue = state
	return nil
}

func TestObserveNoTrace(t *testing.T) {
	t.Parallel()

	cont := &testController{fail: false}
	obs := observer.NewObserver(
		observer.WithInterval(time.Second),
		observer.WithController(cont))
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *receiver.Span)
	go func() { cancel() }()

	obs.Observe(ctx, ch)
	assert.Assert(t, cont.queue == nil)
}

func TestFailStabilize(t *testing.T) {
	t.Parallel()
	spans := []*receiver.Span{
		{
			Duration:    100,
			Parent:      "",
			ServiceName: "service1",
			SpanId:      "span1",
			StartTime:   0,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "service2",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "service2",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace2",
		},
	}

	cont := &testController{fail: true}
	interval := 50 * time.Millisecond
	obs := observer.NewObserver(
		observer.WithInterval(interval),
		observer.WithController(cont))
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *receiver.Span)

	go func() {

		for _, span := range spans {
			ch <- span
		}

		time.Sleep(interval + interval/2)
		cancel()
	}()

	obs.Observe(ctx, ch)
	assert.Assert(t, cont.queue == nil)
}

func TestObserveBeforeTimout(t *testing.T) {
	t.Parallel()

	cont := &testController{fail: false}
	obs := observer.NewObserver(
		observer.WithInterval(time.Second),
		observer.WithController(cont))
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *receiver.Span)

	go func() {
		spans := []*receiver.Span{
			{
				Duration:    100,
				Parent:      "",
				ServiceName: "service1",
				SpanId:      "span1",
				StartTime:   0,
				TraceId:     "trace1",
			},
			{
				Duration:    50,
				Parent:      "span1",
				ServiceName: "service2",
				SpanId:      "span2",
				StartTime:   10,
				TraceId:     "trace1",
			},
		}

		for _, span := range spans {
			ch <- span
		}

		cancel()
	}()

	obs.Observe(ctx, ch)
	assert.Assert(t, cont.queue == nil)
}

func observeTest(expected string, spans [][]*receiver.Span) func(*testing.T) {
	return func(t *testing.T) {
		t.Parallel()

		cont := &testController{fail: false}
		interval := 50 * time.Millisecond
		obs := observer.NewObserver(
			observer.WithInterval(interval),
			observer.WithController(cont))
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan *receiver.Span)

		go func() {
			for _, batch := range spans {
				for _, span := range batch {
					ch <- span
				}

				time.Sleep(interval + interval/2)
			}
			cancel()
		}()

		obs.Observe(ctx, ch)
		assert.Assert(t, cont.queue != nil)
		assert.Equal(t, strings.TrimSpace(expected), cont.queue.ToDOT())
	}
}

func TestObserveApproximation(t *testing.T) {
	spans := [][]*receiver.Span{{
		{
			Duration:    100,
			Parent:      "",
			ServiceName: "service1",
			SpanId:      "span1",
			StartTime:   0,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "service2",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace1",
		},
	}}

	expected := `
digraph {
    ingress [label="ingress"];
    0 [shape=record,label="{service1|mu = 10000000.00 req/s}"];
    1 [shape=record,label="{service2|mu = 20000000.00 req/s}"];
    ingress -> 0 [label="16.00 req/s"];
    0 -> 1 [label="1.00"];
}`
	observeTest(expected, spans)(t)
}

func TestObserveIncompleteTrace(t *testing.T) {
	spans := [][]*receiver.Span{{
		{
			Duration:    100,
			Parent:      "",
			ServiceName: "service1",
			SpanId:      "span1",
			StartTime:   0,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "service2",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "service2",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace2",
		},
	}}

	expected := `
digraph {
    ingress [label="ingress"];
    0 [shape=record,label="{service1|mu = 10000000.00 req/s}"];
    1 [shape=record,label="{service2|mu = 20000000.00 req/s}"];
    ingress -> 0 [label="16.00 req/s"];
    0 -> 1 [label="1.00"];
}`
	observeTest(expected, spans)(t)
}

func TestObserveApproximationNoServiceName(t *testing.T) {
	spans := [][]*receiver.Span{{
		{
			Duration:    100,
			Parent:      "",
			ServiceName: "service1",
			SpanId:      "span1",
			StartTime:   0,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "service2",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace1",
		},
	}}

	expected := `
digraph {
    ingress [label="ingress"];
    0 [shape=record,label="{service1|mu = 10000000.00 req/s}"];
    1 [shape=record,label="{service2|mu = 20000000.00 req/s}"];
    ingress -> 0 [label="16.00 req/s"];
    0 -> 1 [label="1.00"];
}`
	observeTest(expected, spans)(t)
}

func TestObserveIncompleteTraceNoServiceName(t *testing.T) {
	spans := [][]*receiver.Span{{
		{
			Duration:    100,
			Parent:      "",
			ServiceName: "service1",
			SpanId:      "span1",
			StartTime:   0,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "service2",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "service2",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace2",
		},
	}}

	expected := `
digraph {
    ingress [label="ingress"];
    0 [shape=record,label="{service1|mu = 10000000.00 req/s}"];
    1 [shape=record,label="{service2|mu = 20000000.00 req/s}"];
    ingress -> 0 [label="16.00 req/s"];
    0 -> 1 [label="1.00"];
}`
	observeTest(expected, spans)(t)
}

func TestObserveMultipleBatches(t *testing.T) {
	spans := [][]*receiver.Span{{
		{
			Duration:    100,
			Parent:      "",
			ServiceName: "service1",
			SpanId:      "span1",
			StartTime:   0,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "service2",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "service2",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace2",
		},
	}, {
		{
			Duration:    100,
			Parent:      "",
			ServiceName: "service1",
			SpanId:      "span1",
			StartTime:   0,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "service2",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace1",
		},
		{
			Duration:    50,
			Parent:      "span1",
			ServiceName: "service2",
			SpanId:      "span2",
			StartTime:   10,
			TraceId:     "trace1",
		},
	}}

	expected := `
digraph {
    ingress [label="ingress"];
    0 [shape=record,label="{service1|mu = 10000000.00 req/s}"];
    1 [shape=record,label="{service2|mu = 20000000.00 req/s}"];
    ingress -> 0 [label="3.84 req/s"];
    0 -> 1 [label="1.00"];
}`
	observeTest(expected, spans)(t)
}
