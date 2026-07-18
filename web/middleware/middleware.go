// Package middleware provides HTTP safety and observability middleware.
package middleware

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/namanakanchan/sentinel/web/models"
)

// Chain applies request IDs, JSON headers, recovery, and access logging.
func Chain(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.NewString()
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-ID", requestID)
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Error("HTTP panic", "request_id", requestID, "panic", recovered, "stack", string(debug.Stack()))
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(models.Error{Error: "internal server error"})
			}
			logger.Info("HTTP request", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "duration", time.Since(started))
		}()
		next.ServeHTTP(w, r)
	})
}
