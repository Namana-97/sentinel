// Package logger configures structured logging shared by every Sentinel component.
package logger

import (
	"log/slog"
	"os"
)

// New returns a JSON logger suitable for Kubernetes log collection.
func New(level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}
