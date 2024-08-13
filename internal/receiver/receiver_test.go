package receiver_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pako-23/queue-scaler/internal/receiver"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gotest.tools/v3/assert"
)

func TestNewOLTPReceiver(t *testing.T) {
	t.Parallel()

	t.Run("simple constructor", func(t *testing.T) {
		assert.Check(t, receiver.NewOLTPReceiver() != nil)
	})

	t.Run("construct with channel", func(t *testing.T) {
		ch := make(chan *tracepb.ResourceSpans)
		assert.Check(t, receiver.NewOLTPReceiver(receiver.WithChannel(ch)) != nil)
	})

	t.Run("construct with address", func(t *testing.T) {
		assert.Check(t, receiver.NewOLTPReceiver(receiver.WithAddress("127.0.0.0:130")) != nil)
	})

	t.Run("construct with channel and address", func(t *testing.T) {
		ch := make(chan *tracepb.ResourceSpans)
		recv := receiver.NewOLTPReceiver(
			receiver.WithChannel(ch),
			receiver.WithAddress("127.0.0.0:130"))
		assert.Check(t, recv != nil)
	})

}

func assertListening(t *testing.T, lis net.Listener) {
	conn, err := net.DialTimeout("tcp", lis.Addr().String(), time.Second)
	assert.NilError(t, err)
	assert.Check(t, conn != nil)
	conn.Close()

}

func TestStartStop(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var wg sync.WaitGroup

	cond := sync.NewCond(&mu)

	recv := receiver.NewOLTPReceiver(receiver.WithAddress("127.0.0.0:0"))
	assert.Check(t, recv != nil)

	wg.Add(2)

	go func() {
		defer wg.Done()
		lis, ch := recv.Start()
		cond.L.Lock()
		defer cond.L.Unlock()
		assertListening(t, lis)
		cond.Signal()
		cond.Wait()
		assert.NilError(t, <-ch)
	}()
	go func() {
		defer wg.Done()
		cond.L.Lock()
		defer cond.L.Unlock()
		cond.Wait()
		recv.Stop()
		cond.Signal()
	}()

	wg.Wait()
}

func TestFailStart(t *testing.T) {
	t.Parallel()

	recv := receiver.NewOLTPReceiver(receiver.WithAddress("127.0.0.1:1"))
	assert.Check(t, recv != nil)

	lis, ch := recv.Start()
	err := <-ch
	assert.Check(t, err != nil)
	assert.Check(t, lis == nil)

}

func generateTestSpans() map[string]*tracepb.ResourceSpans {
	spans := make(map[string]*tracepb.ResourceSpans, 10)

	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("span-%d", i)

		spans[name] = &tracepb.ResourceSpans{
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					Name:              name,
					StartTimeUnixNano: uint64(time.Now().UnixNano()),
					EndTimeUnixNano:   uint64(time.Now().UnixNano()) + rand.Uint64(),
				}},
			}},
		}
	}

	return spans
}

func TestReceiverSingleSpans(t *testing.T) {
	t.Parallel()

	tests := generateTestSpans()
	ch := make(chan *tracepb.ResourceSpans)
	recv := receiver.NewOLTPReceiver(
		receiver.WithChannel(ch),
		receiver.WithAddress("127.0.0.1:0"))
	assert.Check(t, recv != nil)
	lis, errCh := recv.Start()
	assertListening(t, lis)

	go func() {
		conn, err := grpc.NewClient(lis.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		assert.NilError(t, err)
		defer conn.Close()

		client := coltracepb.NewTraceServiceClient(conn)

		for _, span := range tests {
			res, err := client.Export(context.Background(),
				&coltracepb.ExportTraceServiceRequest{
					ResourceSpans: []*tracepb.ResourceSpans{span}})

			assert.NilError(t, err)
			assert.Equal(t, res.PartialSuccess.ErrorMessage, "")
			assert.Equal(t, res.PartialSuccess.RejectedSpans, int64(0))
		}
	}()

	received := make(map[string]struct{}, len(tests))

	for len(received) != len(tests) {
		select {
		case span := <-ch:
			assert.Equal(t, len(span.ScopeSpans), 1)
			assert.Equal(t, len(span.ScopeSpans[0].Spans), 1)
			name := span.ScopeSpans[0].Spans[0].Name
			if _, ok := received[name]; ok {
				t.Fatal("Received twice the same span")
			}

			testSpan, ok := tests[name]
			assert.Check(t, ok)
			assert.Equal(t, span.ScopeSpans[0].Spans[0].Name,
				testSpan.ScopeSpans[0].Spans[0].Name)
			assert.Equal(t, span.ScopeSpans[0].Spans[0].StartTimeUnixNano,
				testSpan.ScopeSpans[0].Spans[0].StartTimeUnixNano)
			assert.Equal(t, span.ScopeSpans[0].Spans[0].EndTimeUnixNano,
				testSpan.ScopeSpans[0].Spans[0].EndTimeUnixNano)
			received[name] = struct{}{}

		case <-time.After(time.Second):
			t.Fatal("Failed to receive spans")

		case err := <-errCh:
			t.Fatal("Server failed with error: ", err)
		}

	}

	recv.Stop()
}

func TestReceiverMultipleSpans(t *testing.T) {
	t.Parallel()

	tests := generateTestSpans()
	ch := make(chan *tracepb.ResourceSpans)
	recv := receiver.NewOLTPReceiver(
		receiver.WithChannel(ch),
		receiver.WithAddress("127.0.0.1:0"))
	assert.Check(t, recv != nil)
	lis, errCh := recv.Start()
	assertListening(t, lis)

	go func() {
		conn, err := grpc.NewClient(lis.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		assert.NilError(t, err)
		defer conn.Close()

		client := coltracepb.NewTraceServiceClient(conn)
		spans := make([]*tracepb.ResourceSpans, 0, 3)

		submitSpans := func(spans []*tracepb.ResourceSpans) {
			res, err := client.Export(context.Background(),
				&coltracepb.ExportTraceServiceRequest{ResourceSpans: spans})

			assert.NilError(t, err)
			assert.Equal(t, res.PartialSuccess.ErrorMessage, "")
			assert.Equal(t, res.PartialSuccess.RejectedSpans, int64(0))
		}

		for _, span := range tests {
			spans = append(spans, span)
			if len(spans) == 3 {
				submitSpans(spans)
				spans = spans[:0]
			}
		}

		if len(spans) > 0 {
			submitSpans(spans)
		}
	}()

	received := make(map[string]struct{}, len(tests))

	for len(received) != len(tests) {
		select {
		case span := <-ch:
			assert.Equal(t, len(span.ScopeSpans), 1)
			assert.Equal(t, len(span.ScopeSpans[0].Spans), 1)
			name := span.ScopeSpans[0].Spans[0].Name
			if _, ok := received[name]; ok {
				t.Fatal("Received twice the same span")
			}

			testSpan, ok := tests[name]
			assert.Check(t, ok)
			assert.Equal(t, span.ScopeSpans[0].Spans[0].Name,
				testSpan.ScopeSpans[0].Spans[0].Name)
			assert.Equal(t, span.ScopeSpans[0].Spans[0].StartTimeUnixNano,
				testSpan.ScopeSpans[0].Spans[0].StartTimeUnixNano)
			assert.Equal(t, span.ScopeSpans[0].Spans[0].EndTimeUnixNano,
				testSpan.ScopeSpans[0].Spans[0].EndTimeUnixNano)
			received[name] = struct{}{}

		case <-time.After(time.Second):
			t.Fatal("Failed to receive spans")

		case err := <-errCh:
			t.Fatal("Server failed with error: ", err)
		}

	}

	recv.Stop()
}
