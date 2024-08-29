package receiver_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/pako-23/queue-scaler/internal/receiver"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/assert/cmp"
)

func TestNewOLTPReceiver(t *testing.T) {
	t.Parallel()

	t.Run("simple constructor", func(t *testing.T) {
		assert.Assert(t, receiver.NewOLTPReceiver() != nil)
	})

	t.Run("construct with channel", func(t *testing.T) {
		ch := make(chan *receiver.Span)
		assert.Assert(t, receiver.NewOLTPReceiver(receiver.WithChannel(ch)) != nil)
	})

	t.Run("construct with address", func(t *testing.T) {
		assert.Assert(t, receiver.NewOLTPReceiver(receiver.WithAddress("127.0.0.0:130")) != nil)
	})

	t.Run("construct with channel and address", func(t *testing.T) {
		ch := make(chan *receiver.Span)
		recv := receiver.NewOLTPReceiver(
			receiver.WithChannel(ch),
			receiver.WithAddress("127.0.0.0:130"))
		assert.Assert(t, recv != nil)
	})

}

func isListening(lis net.Listener) cmp.Comparison {
	return func() cmp.Result {
		conn, err := net.DialTimeout("tcp", lis.Addr().String(), time.Second)
		if err != nil {
			return cmp.ResultFailure(
				fmt.Sprintf("connection to %s failed with error: %v",
					lis.Addr().String(), err))
		} else if conn == nil {
			return cmp.ResultFailure(fmt.Sprintf("connection to %s failed", lis.Addr().String()))
		}

		return cmp.ResultSuccess
	}
}

func TestStartStop(t *testing.T) {
	t.Parallel()

	recv := receiver.NewOLTPReceiver(receiver.WithAddress("127.0.0.0:0"))
	assert.Assert(t, recv != nil)

	lis, ch := recv.Start()
	assert.Assert(t, isListening(lis))
	recv.Stop()
	assert.NilError(t, <-ch)
}

func TestFailStart(t *testing.T) {
	t.Parallel()

	checkError := func(recv *receiver.OTLPReceiver) {
		assert.Assert(t, recv != nil)

		lis, ch := recv.Start()
		err := <-ch
		assert.Assert(t, err != nil)
		assert.Assert(t, lis == nil)
	}

	t.Run("no channel constructor", func(t *testing.T) {
		checkError(receiver.NewOLTPReceiver(receiver.WithAddress("127.0.0.1:1")))
	})

	t.Run("with channel constructor", func(t *testing.T) {
		ch := make(chan *receiver.Span)
		checkError(receiver.NewOLTPReceiver(
			receiver.WithChannel(ch),
			receiver.WithAddress("127.0.0.1:1")))
	})
}
