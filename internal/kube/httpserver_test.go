package kube

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestHTTPServerGracefulShutdownUnderLoad(t *testing.T) {
	serveStarted := make(chan struct{})
	requestRelease := make(chan struct{})
	serverStopped := make(chan struct{})
	shutdownStarted := make(chan struct{})
	server := HTTPServer{
		Server:          &http.Server{},
		ShutdownTimeout: time.Second,
		Serve: func() error {
			close(serveStarted)
			<-serverStopped
			return http.ErrServerClosed
		},
		Shutdown: func(ctx context.Context) error {
			close(shutdownStarted)
			select {
			case <-requestRelease:
				close(serverStopped)
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Start(ctx) }()
	<-serveStarted
	cancel()
	<-shutdownStarted
	select {
	case err := <-done:
		t.Fatalf("server stopped before in-flight work completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(requestRelease)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("graceful shutdown timed out")
	}
}
