package receiver

import (
	"net"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
)

const DefaultAddress = ":4317"

type Span struct {
	Duration    uint64
	Parent      string
	ServiceName string
	SpanId      string
	StartTime   uint64
	TraceId     string
}

type OTLPReceiver struct {
	server  *grpc.Server
	ch      chan<- *Span
	address string
}

type server struct {
	coltracepb.UnimplementedTraceServiceServer
	ch chan<- *Span
}

type Option func(*OTLPReceiver)

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

func WithChannel(ch chan<- *Span) Option {
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
