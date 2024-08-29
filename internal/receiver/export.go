package receiver

import (
	"context"
	"encoding/hex"

	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func (s *server) Export(
	ctx context.Context, in *coltracepb.ExportTraceServiceRequest,
) (*coltracepb.ExportTraceServiceResponse, error) {
	for _, resourceSpan := range in.ResourceSpans {
		serviceName := extractServiceName(resourceSpan)

		for _, scopeSpan := range resourceSpan.ScopeSpans {
			for _, span := range scopeSpan.Spans {
				s.ch <- &Span{
					Duration:    span.EndTimeUnixNano - span.StartTimeUnixNano,
					Parent:      hex.EncodeToString(span.ParentSpanId),
					ServiceName: serviceName,
					SpanId:      hex.EncodeToString(span.SpanId),
					StartTime:   span.StartTimeUnixNano,
					TraceId:     hex.EncodeToString(span.TraceId),
				}
			}
		}

	}

	return &coltracepb.ExportTraceServiceResponse{
		PartialSuccess: &coltracepb.ExportTracePartialSuccess{
			RejectedSpans: 0,
		},
	}, nil
}

func extractServiceName(spans *tracepb.ResourceSpans) string {
	if spans.Resource == nil || spans.Resource.Attributes == nil {
		return ""
	}

	for _, attribute := range spans.Resource.Attributes {
		if attribute.Key == string(semconv.ServiceNameKey) {
			return attribute.Value.GetStringValue()
		}
	}

	return ""
}
