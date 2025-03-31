package slog

import (
	"context"
	"fmt"
	"log/slog"

	vshardrouter "github.com/tarantool/go-vshard-router/v2"
)

// Check that provider implements LogfProvider interface.
var _ vshardrouter.LogfProvider = (*SlogLoggerf)(nil)

// NewSlogLogger wraps slog logger for go-vshard-router.
func NewSlogLogger(logger *slog.Logger) *SlogLoggerf {
	return &SlogLoggerf{
		Logger: logger,
	}
}

// SlogLoggerf is adapter for slog to Logger interface.
type SlogLoggerf struct {
	Logger *slog.Logger
}

// Debugf implements Debugf method for LogfProvider interface
func (s *SlogLoggerf) Debugf(ctx context.Context, format string, v ...any) {
	if !s.Logger.Enabled(ctx, slog.LevelDebug) {
		return
	}
	s.Logger.DebugContext(ctx, fmt.Sprintf(format, v...))
}

// Infof implements Infof method for LogfProvider interface
func (s *SlogLoggerf) Infof(ctx context.Context, format string, v ...any) {
	if !s.Logger.Enabled(ctx, slog.LevelInfo) {
		return
	}
	s.Logger.InfoContext(ctx, fmt.Sprintf(format, v...))
}

// Warnf implements Warnf method for LogfProvider interface
func (s *SlogLoggerf) Warnf(ctx context.Context, format string, v ...any) {
	if !s.Logger.Enabled(ctx, slog.LevelWarn) {
		return
	}
	s.Logger.WarnContext(ctx, fmt.Sprintf(format, v...))
}

// Errorf implements Errorf method for LogfProvider interface
func (s *SlogLoggerf) Errorf(ctx context.Context, format string, v ...any) {
	if !s.Logger.Enabled(ctx, slog.LevelError) {
		return
	}
	s.Logger.ErrorContext(ctx, fmt.Sprintf(format, v...))
}
