package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/pako-23/queue-scaler/internal/controller"
	"github.com/pako-23/queue-scaler/internal/observer"
	"github.com/pako-23/queue-scaler/internal/receiver"
)

func main() {
	var wg sync.WaitGroup

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	ch := make(chan *receiver.Span)
	recv := receiver.NewOLTPReceiver(receiver.WithChannel(ch))

	cont := controller.NewObserverState()
	httpErr := make(chan error, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, cont.State)
	})
	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	_, recvErr := recv.Start()

	wg.Add(2)
	go func() {
		defer wg.Done()
		obs := observer.NewObserver(observer.WithController(cont))
		obs.Observe(ctx, ch)
	}()
	go func() {
		defer wg.Done()
		httpErr <- server.ListenAndServe()
	}()

	select {
	case err := <-recvErr:
		if err != nil {
			log.Fatalf("failed with error: %v", err)
		}

	case err := <-httpErr:
		if err != nil {
			log.Fatalf("failed with error: %v", err)
		}

	case <-ctx.Done():
		recv.Stop()
		server.Shutdown(ctx)
	}

	wg.Wait()
}
