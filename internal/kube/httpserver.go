package kube

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// HTTPServer adapts net/http to controller-runtime's Runnable lifecycle.
type HTTPServer struct {
	Server          *http.Server
	ShutdownTimeout time.Duration
	Serve           func() error
	Shutdown        func(context.Context) error
}

// Start serves until manager cancellation and performs a bounded graceful shutdown.
func (s HTTPServer) Start(ctx context.Context) error {
	serve := s.Serve
	if serve == nil {
		serve = s.Server.ListenAndServe
	}
	shutdown := s.Shutdown
	if shutdown == nil {
		shutdown = s.Server.Shutdown
	}
	errorsCh := make(chan error, 1)
	go func() { errorsCh <- serve() }()
	select {
	case err := <-errorsCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve REST API: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.ShutdownTimeout)
		defer cancel()
		if err := shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown REST API: %w", err)
		}
		err := <-errorsCh
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("stop REST API: %w", err)
		}
		return nil
	}
}
