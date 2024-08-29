package receiver_test

import (
	"context"
	crypto "crypto/rand"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pako-23/queue-scaler/internal/receiver"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/assert/cmp"
)

const (
	noResource = iota
	nilAttributes
	emptyAttributes
	singleAttribute
	multipleAttributesNoserviceName
	multipleAttributes
)

type testSpans struct {
	spans        []*tracepb.ResourceSpans
	serviceNames []string
}

func generateResource(t *testing.T, serviceName string, generateOption int) (string, *resourcepb.Resource) {
	switch generateOption {
	case noResource:
		return "", nil
	case nilAttributes:
		return "", &resourcepb.Resource{}

	case emptyAttributes:
		return "", &resourcepb.Resource{Attributes: []*commonpb.KeyValue{}}

	case singleAttribute:
		return serviceName, &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{
					Key: string(semconv.ServiceNameKey),
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: serviceName,
						},
					},
				},
			},
		}
	case multipleAttributesNoserviceName:
		resource := &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{},
		}
		size := rand.Intn(10)

		for i := 0; i < size; i++ {
			resource.Attributes = append(resource.Attributes, &commonpb.KeyValue{
				Key: "some key",
				Value: &commonpb.AnyValue{
					Value: &commonpb.AnyValue_StringValue{
						StringValue: "some value",
					},
				},
			})
		}
		return "", resource

	case multipleAttributes:
		resource := &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{},
		}
		size := rand.Intn(10) + 1
		index := rand.Intn(size)

		for i := 0; i < size; i++ {
			if i == index {
				resource.Attributes = append(resource.Attributes, &commonpb.KeyValue{
					Key: string(semconv.ServiceNameKey),
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: serviceName,
						},
					},
				})
			} else {
				resource.Attributes = append(resource.Attributes, &commonpb.KeyValue{
					Key: "some key",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: "some value",
						},
					},
				})
			}
		}

		return serviceName, resource

	default:
		t.Fatal("Unrecognized generation option")
	}

	return "", nil
}

func newTestSpans(t *testing.T, generateOption int) *testSpans {
	spans := &testSpans{
		spans:        make([]*tracepb.ResourceSpans, 0, 10),
		serviceNames: make([]string, 10),
	}

	for i := 0; i < 10; i++ {
		serviceName, resource := generateResource(t, fmt.Sprintf("service-%d", i), generateOption)

		traceId := make([]byte, 20)
		if _, err := crypto.Read(traceId); err != nil {
			t.Fatal("Failed to generate test span trace ID")
		}

		spanId := make([]byte, 20)
		if _, err := crypto.Read(spanId); err != nil {
			t.Fatal("Failed to generate test span span ID")
		}

		parentSpanId := []byte{}
		if rand.Intn(2) == 1 {
			parentSpanId = make([]byte, 20)
			if _, err := crypto.Read(parentSpanId); err != nil {
				t.Fatal("Failed to generate test span parent span ID")
			}
		}

		spans.serviceNames[i] = serviceName

		span := &tracepb.ResourceSpans{
			Resource: resource,
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					StartTimeUnixNano: uint64(time.Now().UnixNano()),
					EndTimeUnixNano:   uint64(time.Now().UnixNano()) + rand.Uint64(),
					TraceId:           traceId,
					SpanId:            spanId,
					ParentSpanId:      parentSpanId,
				}},
			}},
		}
		spans.spans = append(spans.spans, span)
	}

	return spans
}

func (t *testSpans) received(ch <-chan *receiver.Span, errCh <-chan error) cmp.Comparison {
	compareSpans := func(value *receiver.Span, expected *tracepb.ResourceSpans, serviceName string) bool {
		span := expected.ScopeSpans[0].Spans[0]

		return value.Duration == span.EndTimeUnixNano-span.StartTimeUnixNano &&
			value.Parent == hex.EncodeToString(span.ParentSpanId) &&
			value.SpanId == hex.EncodeToString(span.SpanId) &&
			value.ServiceName == serviceName &&
			value.StartTime == span.StartTimeUnixNano &&
			value.TraceId == hex.EncodeToString(span.TraceId)

	}

	return func() cmp.Result {
		processed := make([]bool, len(t.spans))

		for received := 0; received < len(t.spans); received++ {
			select {
			case span := <-ch:

				index := -1
				for i, testSpan := range t.spans {
					if !processed[i] && compareSpans(span, testSpan, t.serviceNames[i]) {
						index = i
						processed[i] = true

						break
					}
				}

				if index == -1 {
					return cmp.ResultFailure(
						fmt.Sprintf("Received unexpected span: %v", span))
				}

			case <-time.After(time.Second):
				return cmp.ResultFailure("Failed to receive spans")

			case err := <-errCh:
				return cmp.ResultFailure(fmt.Sprintf("Server failed with error: %v", err))

			}

		}

		return cmp.ResultSuccess
	}
}

func submitSpansTest(t *testing.T, generateOption int, batchSize int) {
	t.Parallel()

	tests := newTestSpans(t, generateOption)
	ch := make(chan *receiver.Span)
	recv := receiver.NewOLTPReceiver(
		receiver.WithChannel(ch),
		receiver.WithAddress("127.0.0.1:0"))
	assert.Assert(t, recv != nil)
	lis, errCh := recv.Start()
	assert.Assert(t, isListening(lis))

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := grpc.NewClient(lis.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		assert.NilError(t, err)
		defer conn.Close()

		client := coltracepb.NewTraceServiceClient(conn)

		submitSpans := func(spans []*tracepb.ResourceSpans) {
			res, err := client.Export(context.Background(),
				&coltracepb.ExportTraceServiceRequest{ResourceSpans: spans})

			assert.NilError(t, err)
			assert.Equal(t, res.PartialSuccess.ErrorMessage, "")
			assert.Equal(t, res.PartialSuccess.RejectedSpans, int64(0))
		}
		spans := make([]*tracepb.ResourceSpans, 0, batchSize)

		for _, span := range tests.spans {
			spans = append(spans, span)
			if len(spans) == batchSize {
				submitSpans(spans)
				spans = spans[:0]
			}
		}

		if len(spans) > 0 {
			submitSpans(spans)
		}
	}()

	assert.Assert(t, tests.received(ch, errCh))
	wg.Wait()
	recv.Stop()
}

func TestMissingResource(t *testing.T) {
	submitSpansTest(t, noResource, 1)
}

func TestMissingAttributes(t *testing.T) {
	submitSpansTest(t, nilAttributes, 1)
}

func TestEmptyAttributes(t *testing.T) {
	submitSpansTest(t, emptyAttributes, 1)
}

func TestSingleAttribute(t *testing.T) {
	submitSpansTest(t, singleAttribute, 1)
}

func TestNoServiceName(t *testing.T) {
	submitSpansTest(t, multipleAttributesNoserviceName, 1)
}

func TestMultipleAttributes(t *testing.T) {
	submitSpansTest(t, multipleAttributes, 1)
}

func TestMissingResourceBatched(t *testing.T) {
	submitSpansTest(t, noResource, 3)
}

func TestMissingAttributesBatched(t *testing.T) {
	submitSpansTest(t, nilAttributes, 3)
}

func TestEmptyAttributesBatched(t *testing.T) {
	submitSpansTest(t, emptyAttributes, 3)
}

func TestSingleAttributeBatched(t *testing.T) {
	submitSpansTest(t, singleAttribute, 3)
}

func TestNoServiceNameBatched(t *testing.T) {
	submitSpansTest(t, multipleAttributesNoserviceName, 3)
}

func TestMultipleAttributesBatched(t *testing.T) {
	submitSpansTest(t, multipleAttributes, 3)
}
