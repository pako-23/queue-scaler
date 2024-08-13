package receiver

import (
	"context"
	"net"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
)

const DefaultAddress = ":4317"

type OTLPReceiver struct {
	server  *grpc.Server
	ch      chan<- *tracepb.ResourceSpans
	address string
}

type server struct {
	coltracepb.UnimplementedTraceServiceServer
	ch chan<- *tracepb.ResourceSpans
}

type Option func(*OTLPReceiver)

func (s *server) Export(
	ctx context.Context, in *coltracepb.ExportTraceServiceRequest,
) (*coltracepb.ExportTraceServiceResponse, error) {
	for _, span := range in.ResourceSpans {
		s.ch <- span
	}

	return &coltracepb.ExportTraceServiceResponse{
		PartialSuccess: &coltracepb.ExportTracePartialSuccess{
			RejectedSpans: 0,
		},
	}, nil
}

func NewOLTPReceiver(options ...Option) *OTLPReceiver {
	receiver := &OTLPReceiver{
		server:  grpc.NewServer(),
		address: DefaultAddress,
	}

	for _, option := range options {
		option(receiver)
	}

	coltracepb.RegisterTraceServiceServer(receiver.server, &server{ch: receiver.ch})
	return receiver
}

func WithChannel(ch chan<- *tracepb.ResourceSpans) Option {
	return func(receiver *OTLPReceiver) {
		receiver.ch = ch
	}
}

func WithAddress(address string) Option {
	return func(receiver *OTLPReceiver) {
		receiver.address = address
	}
}

func (o *OTLPReceiver) Start() (net.Listener, <-chan error) {
	ch := make(chan error, 1)

	lis, err := net.Listen("tcp", o.address)
	if err != nil {
		ch <- err

		return nil, ch
	}

	go func() {
		ch <- o.server.Serve(lis)
	}()

	return lis, ch
}

func (o *OTLPReceiver) Stop() {
	o.server.Stop()
}
