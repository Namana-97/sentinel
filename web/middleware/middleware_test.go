package middleware

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestIDIsPreservedOrGenerated(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	for _, provided := range []string{"known-id", ""} {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		if provided != "" {
			request.Header.Set("X-Request-ID", provided)
		}
		response := httptest.NewRecorder()
		Chain(logger, next).ServeHTTP(response, request)
		got := response.Header().Get("X-Request-ID")
		if got == "" || (provided != "" && got != provided) {
			t.Fatalf("provided=%q got=%q", provided, got)
		}
	}
}

func TestPanicRecoveryReturnsJSONError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := Chain(logger, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/panic", nil))
	if response.Code != http.StatusInternalServerError || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status=%d headers=%v", response.Code, response.Header())
	}
}
